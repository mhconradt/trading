from collections import deque
from datetime import datetime, timedelta
import dateutil.parser
from decimal import Decimal
import typing as t

from cbpro import AuthenticatedClient
import numpy as np
from pandas import Series

from brain.position import (DesiredLimitBuy, PendingLimitBuy, ActivePosition,
                            DesiredLimitSell, PendingLimitSell, Sold,
                            PendingCancelBuy,
                            PendingCancelLimitSell, DesiredMarketSell,
                            PendingMarketSell,
                            RootState)
from brain.order_tracker import SyncCoinbaseOrderTracker
from brain.volatility_cooldown import VolatilityCoolDown
from indicators import InstantIndicator


# TODO: Rate limiting


class PortfolioManager:
    def __init__(self, client: AuthenticatedClient,
                 price_indicator: InstantIndicator,
                 score_indicator: InstantIndicator):
        self.client = client
        self.price_indicator = price_indicator
        self.score_indicator = score_indicator
        accounts = self.client.get_accounts()
        self.usd_account_id = [account['id'] for account in accounts if
                               account['currency'] == 'USD'][0]
        self.tracker = SyncCoinbaseOrderTracker(client)
        self.min_position_size = Decimal('10')
        self.max_positions = 10_000
        self.position_count = 0

        self.buy_age_limit = timedelta(minutes=1)
        self.sell_age_limit = timedelta(minutes=1)

        # TICK VARIABLES
        self.tick_time: t.Optional[datetime] = None
        self.orders: t.Optional[t.Dict[str, dict]] = None
        self.available_funds: t.Optional[Decimal] = None
        self.prices: t.Optional[Series] = None
        self.scores: t.Optional[Series] = None
        self.market_info: t.Optional[t.Dict[str, dict]] = None
        self.fee: t.Optional[Decimal] = None

        # STATES
        self.desired_limit_buys: deque[DesiredLimitBuy] = deque()
        self.pending_limit_buys: deque[PendingLimitBuy] = deque()
        self.pending_cancel_buys: t.List[PendingCancelBuy] = []
        self.active_positions: t.List[ActivePosition] = []
        self.desired_limit_sells: t.List[DesiredLimitSell] = []
        self.desired_market_sells: t.List[DesiredMarketSell] = []
        self.pending_limit_sells: t.List[PendingLimitSell] = []
        self.pending_market_sells: t.List[PendingMarketSell] = []
        self.pending_cancel_sells: t.List[PendingCancelLimitSell] = []
        self.sells: deque[Sold] = deque()

        self.cool_down = VolatilityCoolDown(timedelta(minutes=5))
        self.next_position_id = 0
        self.gains = Decimal('0')

    def compute_buy_weights(self, scores: Series,
                            spending_limit: Decimal) -> Series:
        nil_weights = Series([], dtype=np.float64)
        if spending_limit < self.min_position_size:
            return nil_weights
        if self.position_count == self.max_positions:
            return nil_weights
        cooling_down = filter(self.cool_down.cooling_down, scores.index)
        scores = scores.loc[scores.index.difference(cooling_down)]
        positive_scores = scores[scores.notna() & scores.gt(0.)]
        ranked_scores = positive_scores.sort_values(ascending=False)
        cumulative_normalized_scores = ranked_scores / ranked_scores.cumsum()
        hypothetical_sizes = cumulative_normalized_scores * spending_limit
        hypothetical_sizes_ok = hypothetical_sizes >= self.min_position_size
        min_position_size_limit = np.arange(hypothetical_sizes_ok).max() + 1
        position_count_limit = self.max_positions - self.position_count
        limit = min(min_position_size_limit, position_count_limit)
        if not limit:
            return nil_weights
        final_scores = ranked_scores.iloc[:limit]
        return final_scores / final_scores.sum()

    def queue_buys(self) -> None:
        """
        Queue buys for the top positive scoring assets.
        Total of price * size should be available_funds after taker_fee.
        Don't queue if would put position count over max_positions.
        Don't queue if would be below min_position_size.

        Don't queue if would put market above market_percentage_limit.
        Don't queue if would violate volatility cooldown.
        """
        spending_limit = self.available_funds / (Decimal('1') + self.fee)
        weights = self.compute_buy_weights(self.scores, spending_limit)
        for market, weight in weights.iteritems():
            assert isinstance(market, str)
            assert isinstance(weight, Decimal)  # TODO: Remove this if it works
            price = self.prices[market]
            size = weight * spending_limit / price
            self.next_position_id += 1
            previous_state = RootState(number=self.next_position_id)
            buy = DesiredLimitBuy(price=price,
                                  size=size,
                                  market=market,
                                  previous_state=previous_state,
                                  state_change=f'buy target {weight:.2f}')
            print(f"{buy}")
            self.desired_limit_buys.append(buy)
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
            if info['status'] != 'online':
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
            if 'id' not in order:
                continue  # Handle this error
            created_at = dateutil.parser.parse(order['created_at'])
            order_id = order['id']
            self.tracker.remember(order_id)
            pending = PendingLimitBuy(price, size, market=market,
                                      order_id=order_id,
                                      created_at=created_at,
                                      previous_state=buy,
                                      state_change='order placed')
            print(f"{pending}")
            self.pending_limit_buys.append(pending)
        # RESET DESIRED BUYS
        self.desired_limit_buys = []

    def check_pending_buys(self) -> None:
        """
        Using "done" and "open" orders.
        Move done orders to active_positions.
        Cancel open orders that are older than age limit.
        If cancelling an order, add the filled_size to active_positions.
        """
        next_generation: t.List[PendingLimitBuy] = []  # Word to Bob
        for pending_buy in self.pending_limit_buys:
            order_id = pending_buy.order_id
            if pending_buy.created_at > self.tick_time:
                # buy was created during this iteration, nothing to do
                next_generation.append(pending_buy)
                continue
            if order_id not in self.orders:
                # buy was canceled externally before being filled
                # candidate explanation is self-trade prevention
                # we don't do anything about this
                self.position_count -= 1
                continue
            order = self.orders[order_id]
            status = order['status']
            # treat these the same?
            if status in {'open', 'pending', 'active'}:
                server_age = pending_buy.created_at - self.tick_time
                time_limit_expired = server_age > self.buy_age_limit
                if time_limit_expired:
                    self.client.cancel_order(order_id)
                    cancel_buy = PendingCancelBuy(
                        market=pending_buy.market,
                        price=pending_buy.price,
                        size=pending_buy.size,
                        order_id=order_id,
                        created_at=pending_buy.created_at,
                        previous_state=pending_buy,
                        state_change=f'age limit {self.buy_age_limit} expired'
                    )
                    print(f"{cancel_buy}")
                    self.pending_cancel_buys.append(cancel_buy)
                else:
                    next_generation.append(pending_buy)
            elif status == 'done':
                self.tracker.forget(order_id)
                size = Decimal(order['filled_size'])
                price = Decimal(order['executed_value']) / size
                fee = Decimal(order['fee'])
                # place stop loss order
                # place take profit order
                # accounting for orders
                active_position = ActivePosition(price, size, fee,
                                                 market=pending_buy.market,
                                                 start=self.tick_time,
                                                 previous_state=pending_buy,
                                                 state_change='order filled')
                print(f"{active_position}")
                self.active_positions.append(active_position)
            else:
                raise ValueError(f"Unknown order status {status}")
        # RESET PENDING BUYS
        self.pending_limit_buys = next_generation

    def check_active_positions(self) -> None:
        """
        Adjust stop losses on active positions.
        Move stop loss triggered positions to desired limit sell.

        """
        next_generation: t.List[ActivePosition] = []
        for position in self.active_positions:
            price = self.prices[position.market]
            stop_loss = position.stop_loss(price)
            take_profit = position.take_profit(price)
            if stop_loss or take_profit:
                state_change = 'stop loss' if stop_loss else 'take profit'
                sell = DesiredLimitSell(price, position.size,
                                        market=position.market,
                                        previous_state=position,
                                        state_change=state_change)
                print(f"{sell}")
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
            if info['status'] != 'online' or info['cancel_only']:
                next_generation.append(sell)  # neanderthal retry
                continue
            elif info['post_only'] or info['limit_only']:
                transition = 'post only' if info['post_only'] else 'limit only'
                limit_sell = DesiredLimitSell(price=self.prices[sell.market],
                                              size=sell.size,
                                              market=sell.market,
                                              previous_state=sell,
                                              state_change=transition)
                print(f"{limit_sell}")
                self.desired_limit_sells.append(limit_sell)
                continue
            order = self.client.place_market_order(sell.market, side='sell',
                                                   size=sell.size)
            if 'id' not in order:
                next_generation.append(sell)  # neanderthal retry
                print(f"DEBUG: Place order error message {order}")
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
            print(f"{pending_sell}")
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
                desired_sell = DesiredMarketSell(market=sell.market,
                                                 size=sell.size,
                                                 previous_state=sell,
                                                 state_change='ext. cancelled')
                print(f"{desired_sell}")
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
                    print(f"{sold}")
                    self.sells.append(sold)
                if remainder:
                    self.position_count += 1
                    transition = 'ext. cancelled'
                    desired_sell = DesiredMarketSell(market=sell.market,
                                                     size=remainder,
                                                     previous_state=sell,
                                                     state_change=transition)
                    print(desired_sell)
                    self.desired_market_sells.append(desired_sell)
            else:
                print(f"Unknown status: {status}")
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
                print(f"DEBUG: Place order error message {order}")
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
            print(f"{pending_sell}")
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
            if sell.created_at > self.tick_time:
                # created during this generation, nothing to see here
                next_generation.append(sell)
                continue
            elif order_id not in self.orders:
                # External cancellation of pending order
                desired_sell = DesiredMarketSell(market=sell.market,
                                                 size=sell.size,
                                                 previous_state=sell,
                                                 state_change='ext. cancel')
                print(f"{desired_sell}")
                self.desired_market_sells.append(desired_sell)
                continue
            order = self.orders[order_id]
            status = order['status']
            if status in {'active', 'pending', 'open'}:
                server_age = self.tick_time - sell.created_at
                time_limit_expired = server_age > self.sell_age_limit
                if time_limit_expired:
                    self.client.cancel_order(order_id)
                    cancellation = PendingCancelLimitSell(
                        market=sell.market,
                        size=sell.size,
                        order_id=order_id,
                        created_at=sell.created_at,
                        previous_state=sell,
                        state_change=f'age limit {self.sell_age_limit} expired'
                    )
                    print(f"{cancellation}")
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
                    print(f"{sold}")
                    self.sells.append(sold)
                if remainder:
                    self.position_count += 1
                    desired_sell = DesiredMarketSell(market=sell.market,
                                                     size=remainder,
                                                     previous_state=sell,
                                                     state_change='ext cancel')
                    print(f"{desired_sell}")
                    self.desired_market_sells.append(desired_sell)
            else:
                print(f"Unknown status: {status}")
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
                print(f"{position}")
                self.active_positions.append(position)
            else:
                print(order)
                print(f"Unknown status {status}")
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
                    buy = DesiredLimitBuy(self.prices.loc[cancellation.market],
                                          remainder, cancellation.market,
                                          previous_state=cancellation,
                                          state_change='partially filled')
                    print(f"{buy}")
                    self.desired_limit_buys.append(buy)
                    self.position_count += 1  # fork
                if order is None or not filled_size:
                    continue
                executed_price = e_v / filled_size
                filled = filled_size == cancellation.size
                state_change = 'filled' if filled else 'partial fill'
                sell = Sold(price=executed_price, size=filled_size,
                            fees=Decimal(order['fee']),
                            market=cancellation.market,
                            previous_state=cancellation,
                            state_change=state_change)
                print(f"{sell}")
                self.sells.append(sell)
                continue
            elif order['status'] in {'active', 'pending', 'open'}:
                next_generation.append(cancellation)
                continue
        self.pending_cancel_sells = next_generation

    def check_sold(self) -> None:
        """
        Record final information about each position.
        """
        while self.sells:
            sell = self.sells.popleft()
            state = sell
            while state:
                if isinstance(state, ActivePosition):
                    gain = (sell.price - state.price) * sell.size
                    print(f"{sell.market}: ${gain}")
                    break
                state = state.previous_state
            self.position_count -= 1

    def set_tick_variables(self) -> None:
        usd_account = self.client.get_account(self.usd_account_id)
        self.available_funds = Decimal(usd_account['available'])
        self.prices = self.price_indicator.compute().map(Decimal)
        self.scores = self.score_indicator.compute().map(Decimal)
        self.market_info = {product['id']: product for product in
                            self.client.get_products()}
        self.tick_time, self.orders = self.tracker.barrier_snapshot()
        send_message = getattr(self.client, '_send_message')
        fee_info = send_message('GET', '/fees')
        self.fee = Decimal(fee_info['taker_fee'])

    def run(self) -> t.NoReturn:
        last_tick = dateutil.parser.parse(self.client.get_time()['iso'])
        while True:
            self.set_tick_variables()
            if not self.tick_time > last_tick:
                # wait for order snapshot to catch up
                # this should never happen with the synchronous order tracker
                continue
            self.cool_down.set_tick(self.tick_time)
            self.manage_positions()
            last_tick = dateutil.parser.parse(self.client.get_time()['iso'])

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
        self.queue_buys()
