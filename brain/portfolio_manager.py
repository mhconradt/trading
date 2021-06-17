import logging
import typing as t
from collections import deque
from datetime import datetime, timedelta
from decimal import Decimal

import dateutil.parser
import numpy as np
import requests
from cbpro import AuthenticatedClient
from pandas import Series

from brain.order_tracker import SyncCoinbaseOrderTracker
from brain.position import (DesiredLimitBuy, PendingLimitBuy, ActivePosition,
                            DesiredLimitSell, PendingLimitSell, Sold,
                            PendingCancelBuy,
                            PendingCancelLimitSell, DesiredMarketSell,
                            PendingMarketSell,
                            RootState)
from brain.stop_loss import SimpleStopLoss
from brain.volatility_cooldown import VolatilityCoolDown
from helper.coinbase import get_server_time
from indicators import InstantIndicator

# TODO: Rate limiting
logger = logging.getLogger(__name__)


class PortfolioManager:
    def __init__(self, client: AuthenticatedClient,
                 price_indicator: InstantIndicator,
                 score_indicator: InstantIndicator, stop_loss: SimpleStopLoss,
                 cool_down: VolatilityCoolDown,
                 market_blacklist: t.Container[str]):
        self.client = client
        self.price_indicator = price_indicator
        self.score_indicator = score_indicator
        accounts = self.client.get_accounts()
        self.usd_account_id = [account['id'] for account in accounts if
                               account['currency'] == 'USD'][0]
        self.tracker = SyncCoinbaseOrderTracker(client)
        # a buy is allocated only until it receives a server response
        self.allocations = Decimal('0')
        # avoid congesting system with dozens of tiny positions
        self.min_position_size = Decimal('10')
        self.max_positions = 100
        self.position_count = 0

        self.buy_age_limit = timedelta(minutes=1)
        self.sell_age_limit = timedelta(minutes=1)

        # TICK VARIABLES
        self.tick_time: t.Optional[datetime] = None
        self.orders: t.Optional[t.Dict[str, dict]] = None
        self.portfolio_available_funds: t.Optional[Decimal] = None
        self.prices: t.Optional[Series] = None
        self.scores: t.Optional[Series] = None
        self.market_info: t.Optional[t.Dict[str, dict]] = None
        self.fee: t.Optional[Decimal] = None

        # STATES
        self.desired_limit_buys: deque[DesiredLimitBuy] = deque()
        self.pending_limit_buys: t.List[PendingLimitBuy] = []
        self.pending_cancel_buys: t.List[PendingCancelBuy] = []
        self.active_positions: t.List[ActivePosition] = []
        self.desired_limit_sells: t.List[DesiredLimitSell] = []
        self.desired_market_sells: t.List[DesiredMarketSell] = []
        self.pending_limit_sells: t.List[PendingLimitSell] = []
        self.pending_market_sells: t.List[PendingMarketSell] = []
        self.pending_cancel_sells: t.List[PendingCancelLimitSell] = []
        self.sells: t.List[Sold] = []

        self.cool_down = cool_down
        self.stop_loss = stop_loss
        self.next_position_id = 0
        self.gains = Decimal('0')
        self.blacklist = market_blacklist

    @property
    def budget(self) -> Decimal:
        return self.portfolio_available_funds - self.allocations

    def compute_buy_weights(self, scores: Series,
                            spending_limit: Decimal) -> Series:
        nil_weights = Series([], dtype=np.float64)
        if spending_limit < self.min_position_size:
            return nil_weights
        if self.position_count == self.max_positions:
            return nil_weights
        cooling_down = filter(self.cool_down.cooling_down, scores.index)
        allowed = scores.index.difference(cooling_down)
        allowed = allowed.difference(self.blacklist)
        scores = scores.loc[allowed]
        positive_scores = scores[scores.notna() & scores.gt(0.)]
        if not len(positive_scores):
            return nil_weights
        ranked_scores = positive_scores.sort_values(ascending=False)
        cumulative_normalized_scores = ranked_scores / ranked_scores.cumsum()
        hypothetical_sizes = cumulative_normalized_scores * spending_limit
        hypothetical_sizes_ok = hypothetical_sizes >= self.min_position_size
        min_position_size_limit = int(hypothetical_sizes_ok.sum())
        position_count_limit = self.max_positions - self.position_count
        limit = min(min_position_size_limit, position_count_limit)
        if not limit:
            return nil_weights
        final_scores = cumulative_normalized_scores.iloc[:limit]
        weights = final_scores / final_scores.sum()
        logger.debug(weights)
        return weights

    def queue_buys(self) -> None:
        """
        Queue buys for the top positive scoring assets.
        Total of price * size should be available_funds after taker_fee.
        Don't queue if would put position count over max_positions.
        Don't queue if would be below min_position_size.

        Don't queue if would put market above market_percentage_limit.
        Don't queue if would violate volatility cooldown.
        """
        starting_budget = self.budget  # this computed property changes in loop
        spending_limit = starting_budget / (Decimal('1') + self.fee)
        weights = self.compute_buy_weights(self.scores, spending_limit)
        decimal_weights = weights.map(Decimal)
        for market, weight in decimal_weights.iteritems():
            assert isinstance(market, str)
            price = self.prices[market]
            size = weight * spending_limit / price
            self.next_position_id += 1
            allocation = weight * starting_budget
            previous_state = RootState(number=self.next_position_id)
            buy = DesiredLimitBuy(price=price,
                                  size=size,
                                  market=market,
                                  allocation=allocation,
                                  previous_state=previous_state,
                                  state_change=f'buy target {weight:.2f}')
            self.allocations += allocation
            logger.debug(f"{buy}")
            self.desired_limit_buys.appendleft(buy)
            self.position_count += 1
        return None

    def check_desired_buys(self) -> None:
        """
        Place GTC orders for desired buys and add to pending buys queue.

        Only place orders for markets that are online.
        Only place orders that are within exchange limits for market.

        Percentage-of-volume trading at some point?
        """
        for buy in self.desired_limit_buys:
            market = buy.market
            info = self.market_info[market]
            if info['status'] != 'online' or info['trading_disabled']:
                continue
                # could probably use post_only in the order flags
            elif info['cancel_only'] or info['post_only']:
                continue
            price = buy.price.quantize(Decimal(info['quote_increment']))
            size = buy.size.quantize(Decimal(info['base_increment']))
            min_size = Decimal(info['base_min_size'])
            if size < min_size:
                continue
            max_size = Decimal(info['base_max_size'])
            size = min(size, max_size)
            order = self.client.place_limit_order(market, side='buy',
                                                  price=str(price),
                                                  size=str(size),
                                                  time_in_force='GTC',
                                                  stp='cn')
            self.cool_down.bought(market)
            self.allocations -= buy.allocation  # we tried
            if 'id' not in order:
                logger.warning(order)
                continue  # This means there was a problem with the order
            created_at = dateutil.parser.parse(order['created_at'])
            order_id = order['id']
            self.tracker.remember(order_id)
            pending = PendingLimitBuy(price, size, market=market,
                                      order_id=order_id,
                                      created_at=created_at,
                                      previous_state=buy,
                                      state_change='order placed')
            logger.debug(f"{pending}")
            self.pending_limit_buys.append(pending)
        # RESET DESIRED BUYS
        self.desired_limit_buys = deque()

    def check_pending_buys(self) -> None:
        """
        Using "done" and "open" orders.
        Move done orders to active_positions.
        Cancel open orders that are older than age limit.
        If cancelling an order, add the filled_size to active_positions.
        """
        next_generation: t.List[PendingLimitBuy] = []  # Word to Bob
        for pending_buy in self.pending_limit_buys:
            if self.market_info[pending_buy.market]['trading_disabled']:
                next_generation.append(pending_buy)
                continue
            order_id = pending_buy.order_id
            if pending_buy.created_at > self.tick_time:
                # buy was created during this iteration, nothing to do
                next_generation.append(pending_buy)
                continue
            if order_id not in self.orders:
                # buy was canceled externally before being filled
                # candidate explanation is self-trade prevention
                # we don't do anything about this
                self.tracker.forget(order_id)
                self.position_count -= 1
                continue
            order = self.orders[order_id]
            status = order['status']
            # treat these the same?
            if status in {'open', 'pending', 'active'}:
                server_age = pending_buy.created_at - self.tick_time
                time_limit_expired = server_age > self.buy_age_limit
                if time_limit_expired:
                    try:
                        self.client.cancel_order(order_id)
                    except requests.RequestException:
                        next_generation.append(pending_buy)
                        continue
                    cancel_buy = PendingCancelBuy(
                        market=pending_buy.market,
                        price=pending_buy.price,
                        size=pending_buy.size,
                        order_id=order_id,
                        created_at=pending_buy.created_at,
                        previous_state=pending_buy,
                        state_change=f'age limit {self.buy_age_limit} expired'
                    )
                    logger.debug(f"{cancel_buy}")
                    self.pending_cancel_buys.append(cancel_buy)
                else:
                    next_generation.append(pending_buy)
            elif status == 'done':
                self.tracker.forget(order_id)
                size = Decimal(order['filled_size'])
                price = Decimal(order['executed_value']) / size
                fee = Decimal(order['fill_fees'])
                # place stop loss order
                # place take profit order
                # accounting for orders
                active_position = ActivePosition(price, size, fee,
                                                 market=pending_buy.market,
                                                 start=self.tick_time,
                                                 previous_state=pending_buy,
                                                 state_change='order filled')
                logger.debug(f"{active_position}")
                self.active_positions.append(active_position)
            else:
                logger.warning(f"Unknown status {status}.")
                logger.debug(order)
                next_generation.append(pending_buy)
        # RESET PENDING BUYS
        self.pending_limit_buys = next_generation

    def check_active_positions(self) -> None:
        """
        Adjust stop losses on active positions.
        Move stop loss triggered positions to desired limit sell.

        """
        next_generation: t.List[ActivePosition] = []
        for position in self.active_positions:
            if position.market not in self.prices:
                next_generation.append(position)
                continue
            price = self.prices[position.market]
            price_paid = position.price
            stop_loss = self.stop_loss.trigger_stop_loss(price, price_paid)
            take_profit = self.stop_loss.trigger_take_profit(price, price_paid)
            if stop_loss or take_profit:
                state_change = 'stop loss' if stop_loss else 'take profit'
                sell = DesiredLimitSell(price, position.size,
                                        market=position.market,
                                        previous_state=position,
                                        state_change=state_change)
                logger.debug(f"{sell}")
                self.cool_down.sold(position.market)
                self.desired_limit_sells.append(sell)
            else:
                next_generation.append(position)
        self.active_positions = next_generation

    def check_desired_market_sells(self) -> None:
        """
        Place market sell orders for desired sells.
        """
        next_generation: t.List[DesiredMarketSell] = []
        for sell in self.desired_market_sells:
            info = self.market_info[sell.market]
            if info['trading_disabled']:
                next_generation.append(sell)  # neanderthal retry
                continue
            if info['status'] != 'online' or info['cancel_only']:
                next_generation.append(sell)  # neanderthal retry
                continue
            elif info['post_only'] or info['limit_only']:
                transition = 'post only' if info['post_only'] else 'limit only'
                if sell.market not in self.prices:
                    next_generation.append(sell)  # neanderthal retry
                    continue
                limit_sell = DesiredLimitSell(price=self.prices[sell.market],
                                              size=sell.size,
                                              market=sell.market,
                                              previous_state=sell,
                                              state_change=transition)
                logger.debug(f"{limit_sell}")
                self.desired_limit_sells.append(limit_sell)
                continue
            order = self.client.place_market_order(sell.market,
                                                   side='sell',
                                                   size=sell.size)
            if 'id' not in order:
                next_generation.append(sell)  # neanderthal retry
                logger.debug(f"Place order error message {order}")
                continue
            order_id = order['id']
            self.tracker.remember(order_id)
            created_at = dateutil.parser.parse(order['created_at'])
            pending_sell = PendingMarketSell(size=sell.size,
                                             market=sell.market,
                                             order_id=order_id,
                                             created_at=created_at,
                                             previous_state=sell,
                                             state_change='order created')
            logger.debug(f"{pending_sell}")
            self.pending_market_sells.append(pending_sell)
        self.desired_market_sells = next_generation

    def check_pending_market_sells(self) -> None:
        """
        Monitor pending market sell orders.
        """
        next_generation: t.List[PendingMarketSell] = []
        for sell in self.pending_market_sells:
            order_id = sell.order_id
            if sell.created_at > self.tick_time:
                next_generation.append(sell)
                continue
            elif order_id not in self.orders:
                self.tracker.forget(order_id)
                desired_sell = DesiredMarketSell(market=sell.market,
                                                 size=sell.size,
                                                 previous_state=sell,
                                                 state_change='ext. cancelled')
                logger.debug(f"{desired_sell}")
                self.desired_market_sells.append(desired_sell)
                continue
            order = self.orders[order_id]
            status = order['status']
            if status in {'pending', 'active', 'open'}:
                next_generation.append(sell)
                continue
            elif status == 'done':
                self.tracker.forget(order_id)
                size = Decimal(order['size'])
                filled_size = Decimal(order['filled_size'])
                self.position_count -= 1
                remainder = size - filled_size
                if filled_size:
                    self.position_count += 1
                    executed_value = Decimal(order['executed_value'])
                    executed_price = executed_value / filled_size
                    fee = Decimal(order['fee'])
                    transition = 'fill' if not remainder else 'partial fill'
                    sold = Sold(market=sell.market, size=filled_size,
                                price=executed_price, fees=fee,
                                previous_state=sell,
                                state_change=transition)
                    logger.debug(f"{sold}")
                    self.sells.append(sold)
                if remainder:
                    self.position_count += 1
                    transition = 'ext. cancelled'
                    desired_sell = DesiredMarketSell(market=sell.market,
                                                     size=remainder,
                                                     previous_state=sell,
                                                     state_change=transition)
                    logger.debug(f"{desired_sell}")
                    self.desired_market_sells.append(desired_sell)
            else:
                logger.warning(f"Unknown status: {status}")
                logger.debug(order)
                next_generation.append(sell)
        self.pending_market_sells = next_generation

    def check_desired_limit_sells(self) -> None:
        """
        Place limit sell orders for desired sells.
        """
        next_generation: t.List[DesiredLimitSell] = []
        for sell in self.desired_limit_sells:
            # TODO: Fork on max size limit reached
            market_info = self.market_info[sell.market]
            if market_info['trading_disabled']:
                next_generation.append(sell)
                continue
            quote_increment = Decimal(market_info['quote_increment'])
            price = sell.price.quantize(quote_increment)
            post_only = market_info['post_only']
            order = self.client.place_limit_order(sell.market, side='sell',
                                                  price=str(price),
                                                  size=str(sell.size),
                                                  time_in_force='GTC',
                                                  stp='co',
                                                  post_only=post_only)
            if 'id' not in order:
                next_generation.append(sell)
                logger.debug(f"Place order error message {order}")
                continue
            order_id = order['id']
            self.tracker.remember(order_id)
            created_at = dateutil.parser.parse(order['created_at'])
            pending_sell = PendingLimitSell(price=price, size=sell.size,
                                            market=sell.market,
                                            order_id=order_id,
                                            created_at=created_at,
                                            previous_state=sell,
                                            state_change='order placed')
            logger.debug(f"{pending_sell}")
            self.pending_limit_sells.append(pending_sell)
        self.desired_limit_sells = next_generation

    def check_pending_limit_sells(self) -> None:
        """
        Using "done" and "open" orders.
        Move positions whose orders are "done" to sold positions.
        Move positions whose orders are open to pending sells.
        """
        next_generation: t.List[PendingLimitSell] = []
        for sell in self.pending_limit_sells:
            order_id = sell.order_id
            trading_disabled = self.market_info[sell.market]
            if sell.created_at > self.tick_time:
                # created during this generation, nothing to see here
                next_generation.append(sell)
                continue
            elif order_id not in self.orders:
                self.tracker.forget(order_id)
                # External cancellation of pending order
                desired_sell = DesiredMarketSell(market=sell.market,
                                                 size=sell.size,
                                                 previous_state=sell,
                                                 state_change='ext. cancel')
                logger.debug(f"{desired_sell}")
                self.desired_market_sells.append(desired_sell)
                continue
            order = self.orders[order_id]
            status = order['status']
            if status in {'active', 'pending', 'open'}:
                server_age = self.tick_time - sell.created_at
                time_limit_expired = server_age > self.sell_age_limit
                if time_limit_expired and not trading_disabled:
                    try:
                        self.client.cancel_order(order_id)
                    except requests.RequestException:
                        next_generation.append(sell)
                        continue
                    cancellation = PendingCancelLimitSell(
                        market=sell.market,
                        size=sell.size,
                        order_id=order_id,
                        created_at=sell.created_at,
                        previous_state=sell,
                        state_change=f'age limit {self.sell_age_limit} expired'
                    )
                    logger.debug(f"{cancellation}")
                    self.pending_cancel_sells.append(cancellation)
                else:
                    next_generation.append(sell)
                    continue
            elif status == 'done':
                self.tracker.forget(order_id)
                # external cancellation without being filled
                executed_value = Decimal(order['executed_value'])
                size = Decimal(order['size'])
                filled_size = Decimal(order['filled_size'])
                self.position_count -= 1
                remainder = size - filled_size
                if filled_size:
                    self.position_count += 1
                    state_change = 'partial fill' if remainder else 'filled'
                    executed_price = executed_value / filled_size
                    sold = Sold(price=executed_price, size=filled_size,
                                fees=Decimal(order['fee']),
                                market=sell.market,
                                previous_state=sell,
                                state_change=state_change,
                                )
                    logger.debug(f"{sold}")
                    self.sells.append(sold)
                if remainder:
                    self.position_count += 1
                    desired_sell = DesiredMarketSell(market=sell.market,
                                                     size=remainder,
                                                     previous_state=sell,
                                                     state_change='ext cancel')
                    logger.debug(f"{desired_sell}")
                    self.desired_market_sells.append(desired_sell)
            else:
                logger.warning(f"Unknown status: {status}")
                logger.debug(order)
                next_generation.append(sell)
                continue
        self.pending_limit_sells = next_generation

    def check_cancel_buys(self) -> None:
        next_generation: t.List[PendingCancelBuy] = []
        for cancellation in self.pending_cancel_buys:
            order_id = cancellation.order_id
            if order_id not in self.orders:
                self.tracker.forget(order_id)
                self.position_count -= 1  # canceled without fill
                continue
            order = self.orders[order_id]
            status = order['status']
            if status == {'active', 'pending', 'open'}:
                next_generation.append(cancellation)
                continue
            elif status == 'done':
                self.tracker.forget(order_id)
                executed_value = Decimal(order['executed_value'])
                filled_size = Decimal(order['filled_size'])
                fully_filled = filled_size == cancellation.size
                state_change = 'filled' if fully_filled else 'partial fill'
                executed_price = executed_value / filled_size
                position = ActivePosition(market=cancellation.market,
                                          price=executed_price,
                                          size=filled_size,
                                          fees=Decimal(order['fee']),
                                          start=self.tick_time,
                                          previous_state=cancellation,
                                          state_change=state_change)
                logger.debug(f"{position}")
                self.active_positions.append(position)
            else:
                logger.debug(order)
                logger.debug(f"Unknown status {status}")
                next_generation.append(cancellation)
                continue
        self.pending_cancel_buys = next_generation

    def check_cancel_sells(self) -> None:
        next_generation: t.List[PendingCancelLimitSell] = []
        for cancellation in self.pending_cancel_sells:
            order_id = cancellation.order_id
            order = self.orders.get(order_id, None)
            if order is None or order['status'] == 'done':
                self.tracker.forget(order_id)
                e_v = Decimal(order['executed_value'] if order else '0')
                filled_size = Decimal(order['filled_size'] if order else '0')
                remainder = cancellation.size - filled_size
                if remainder:
                    buy = DesiredMarketSell(
                        size=remainder, market=cancellation.market,
                        previous_state=cancellation,
                        state_change='partially filled')
                    logger.debug(f"{buy}")
                    self.desired_market_sells.append(buy)
                    self.position_count += 1  # fork
                if order is None or not filled_size:
                    self.position_count -= 1
                    continue
                executed_price = e_v / filled_size
                filled = filled_size == cancellation.size
                state_change = 'filled' if filled else 'partial fill'
                sell = Sold(price=executed_price, size=filled_size,
                            fees=Decimal(order['fee']),
                            market=cancellation.market,
                            previous_state=cancellation,
                            state_change=state_change)
                logger.debug(f"{sell}")
                self.sells.append(sell)
            elif order['status'] in {'active', 'pending', 'open'}:
                next_generation.append(cancellation)
                continue
        self.pending_cancel_sells = next_generation

    def check_sold(self) -> None:
        """
        Record final information about each position.
        """
        while self.sells:
            sell = self.sells.pop()
            state = sell
            while state:
                if isinstance(state, ActivePosition):
                    gain = (sell.price - state.price) * sell.size
                    print(f"Sold! {sell.market}: ${gain}")
                    break
                state = state.previous_state
            self.position_count -= 1

    from retry import retry

    @retry(requests.RequestException, tries=5, delay=15)
    def set_tick_variables(self) -> None:
        self.set_portfolio_available_funds()
        self.prices = self.price_indicator.compute().map(Decimal)
        self.scores = self.score_indicator.compute().map(Decimal)
        self.market_info = {product['id']: product for product in
                            self.client.get_products()}
        self.tick_time, self.orders = self.tracker.barrier_snapshot()
        send_message = getattr(self.client, '_send_message')
        fee_info = send_message('GET', '/fees')
        self.fee = Decimal(fee_info['taker_fee_rate'])

    @retry(requests.RequestException, tries=5, delay=15)
    def set_portfolio_available_funds(self):
        usd_account = self.client.get_account(self.usd_account_id)
        self.portfolio_available_funds = Decimal(usd_account['available'])

    @retry(requests.RequestException, tries=5, delay=15)
    def shutdown(self) -> None:
        logger.info(f"Shutting down...")
        self.client.cancel_all()
        for account in self.client.get_accounts():
            # TODO: Lots of potential here
            currency = account['currency']
            if currency == 'USD':
                continue
            product = f'{account["currency"]}-USD'
            available = account['available']
            if Decimal(available):
                self.client.place_market_order(product,
                                               side='sell',
                                               size=available)
                logger.info(f"Placed market sell for {available} {currency}")

    def run(self) -> t.NoReturn:
        last_tick = get_server_time()
        while True:
            self.set_tick_variables()
            if not self.tick_time > last_tick:
                # wait for order snapshot to catch up
                # this should never happen with the synchronous order tracker
                logger.warning("backing off")
                continue
            self.cool_down.set_tick(self.tick_time)
            self.manage_positions()
            last_tick = get_server_time()

    def manage_positions(self):
        self.check_sold()
        self.check_cancel_sells()
        self.check_pending_market_sells()
        self.check_pending_limit_sells()
        self.check_desired_market_sells()
        self.check_desired_limit_sells()
        self.check_active_positions()
        self.check_cancel_buys()
        self.check_pending_buys()
        self.check_desired_buys()

        self.set_portfolio_available_funds()
        self.queue_buys()


__all__ = ['PortfolioManager']
