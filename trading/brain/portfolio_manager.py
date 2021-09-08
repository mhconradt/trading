import itertools as it
import logging
import time
import typing as t
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal

import dateutil.parser
import numpy as np
import pandas as pd
from pandas import DataFrame, Series

from trading.brain.cool_down import CoolDown
from trading.brain.position import (DesiredLimitBuy, PendingLimitBuy,
                                    ActivePosition,
                                    DesiredLimitSell, PendingLimitSell, Sold,
                                    DesiredMarketSell,
                                    PendingMarketSell,
                                    PendingMarketBuy, DesiredMarketBuy,
                                    Download, RootState)
from trading.brain.position_counter import PositionCounter
from trading.brain.position_sizing import limit_limit_buy_size, \
    limit_market_buy_size, compute_sell_size, adjust_spending_target
from trading.brain.stop_loss import StopLoss
from trading.coinbase.helper import get_server_time, AuthenticatedClient
from trading.helper.functions import overlapping_labels, safely_decimalize
from trading.indicators.protocols import InstantIndicator, BidAskIndicator, \
    CandlesIndicator
from trading.order_tracker import OrderTracker

ORDER_WAIT_TIME = timedelta(seconds=1)

logger = logging.getLogger(__name__)


class PortfolioManager:
    def __init__(self, exchange_client: AuthenticatedClient,
                 candles_src: CandlesIndicator,
                 buy_indicator: InstantIndicator,
                 sell_indicator: InstantIndicator,
                 price_indicator: InstantIndicator,
                 volume_indicator: InstantIndicator,
                 bid_ask_indicator: BidAskIndicator,
                 market_blacklist: t.Iterable[str],
                 liquidate_on_shutdown: bool, quote: str,
                 order_tracker: OrderTracker, cool_down: CoolDown,
                 stop_loss: StopLoss, *,
                 sell_horizon=timedelta(minutes=5),
                 buy_time_in_force: str = 'GTC',
                 sell_time_in_force: str = 'GTC',
                 buy_age_limit=timedelta(minutes=1),
                 sell_age_limit=timedelta(minutes=1), post_only: bool = False,
                 sell_order_type: str = 'limit', buy_order_type: str = 'limit',
                 buy_horizon=timedelta(minutes=10),
                 min_tick_time: float = 0.,
                 concentration_limit: float = 0.25):
        # COINBASE CLIENT
        self.exchange = exchange_client
        # SPENDING DIRECTIVES
        self.quote = quote
        self.buy_horizon = buy_horizon
        self.sell_horizon = sell_horizon
        self.blacklist = market_blacklist
        # LEVEL1 INDICATORS
        self.candles_src = candles_src
        self.price_indicator = price_indicator
        self.volume_indicator = volume_indicator
        self.buy_indicator = buy_indicator
        self.sell_indicator = sell_indicator
        self.bid_ask_indicator = bid_ask_indicator
        # POSITION TRACKING
        self.tracker = order_tracker
        self.counter = PositionCounter()  # Worthless except for catching bugs
        # RISK MANAGEMENT
        self.cool_down = cool_down
        self.stop_loss = stop_loss
        self.concentration_limit = Decimal(concentration_limit)
        self.pov_limit = Decimal('1')
        # TICK VARIABLES
        self.tick_time: t.Optional[datetime] = None
        self.order_snapshot_time: t.Optional[datetime] = None
        self.orders: t.Optional[t.Dict[str, dict]] = None
        self.portfolio_available_funds: t.Optional[Decimal] = None
        # ORDER INSTRUCTIONS
        self.post_only = post_only
        self.buy_age_limit = buy_age_limit
        self.sell_age_limit = sell_age_limit
        valid_order_types = {'limit', 'market'}
        if buy_order_type not in valid_order_types:
            raise ValueError(f"Invalid order type {buy_order_type}")
        self.buy_order_type = buy_order_type
        if sell_order_type not in valid_order_types:
            raise ValueError(f"Invalid order type {sell_order_type}")
        self.sell_order_type = sell_order_type
        # TIMES IN FORCE
        valid_times_in_force = {'FOK', 'GTC', 'IOC'}
        if buy_time_in_force not in valid_times_in_force:
            raise ValueError(f"Invalid time in force {buy_time_in_force}")
        self.buy_time_in_force = buy_time_in_force
        if sell_time_in_force not in valid_times_in_force:
            raise ValueError(f"Invalid time in force {sell_time_in_force}")
        self.sell_time_in_force = sell_time_in_force
        # LEVEL1 / COMPUTED MARKET DATA
        self.prices: t.Optional[Series] = None
        self.bids: t.Optional[Series] = None
        self.asks: t.Optional[Series] = None
        self.volume: t.Optional[Series] = None
        self.buy_weights: t.Optional[Series] = None
        self.sell_weights: t.Optional[Series] = None
        # (APPROXIMATELY) STATIC MARKET/PORTFOLIO DATA
        accounts = self.exchange.get_accounts()
        self.quote_account_id = [account['id'] for account in accounts if
                                 account['currency'] == self.quote][0]
        self.market_info: t.Optional[t.Dict[str, dict]] = None
        self.taker_fee: t.Optional[Decimal] = None
        self.maker_fee: t.Optional[Decimal] = None
        # STATES
        self.desired_limit_buys: t.List[DesiredLimitBuy] = []
        self.desired_market_buys: t.List[DesiredMarketBuy] = []
        self.pending_limit_buys: t.List[PendingLimitBuy] = []
        self.pending_market_buys: t.List[PendingMarketBuy] = []
        self.active_positions: t.List[ActivePosition] = []
        self.desired_limit_sells: t.List[DesiredLimitSell] = []
        self.desired_market_sells: t.List[DesiredMarketSell] = []
        self.pending_limit_sells: t.List[PendingLimitSell] = []
        self.pending_market_sells: t.List[PendingMarketSell] = []
        self.sells: t.List[Sold] = []
        # CONTROL FLOW DIRECTIVES
        self.liquidate_on_shutdown = liquidate_on_shutdown
        self.stop = False
        self.initialized = False
        self.min_tick_time = min_tick_time

    @property
    def aum(self) -> Decimal:
        quote_sizes = self.calculate_position_quote_sizes()
        total_size = np.sum(quote_sizes)
        return total_size + self.portfolio_available_funds

    def calculate_position_quote_sizes(self) -> pd.Series:
        sizes = defaultdict(lambda: Decimal('0'))
        positions = it.chain(self.desired_limit_buys, self.pending_limit_buys,
                             self.desired_market_buys,
                             self.pending_market_buys,
                             self.active_positions,
                             self.desired_limit_sells,
                             self.desired_market_sells)
        for position in positions:
            if isinstance(position, (PendingMarketBuy, DesiredMarketBuy)):
                sizes[position.market] += position.funds
            elif position.market in self.prices:
                price = self.prices[position.market]
                sizes[position.market] += position.size * price
        return Series({market: sizes[market] for market in self.market_info})

    @property
    def spending_limits(self) -> pd.Series:
        size_limits = self.position_size_limits
        current_sizes = self.calculate_position_quote_sizes()
        remaining = size_limits - current_sizes
        min_limit = Decimal('0')
        # if a position price goes up then remaining could be negative
        spending_limits = remaining.where(remaining >= min_limit, min_limit)
        return spending_limits.fillna(Decimal('0')).map(Decimal)

    @property
    def position_size_limits(self) -> pd.Series:
        aum_size_limit = self.aum * self.concentration_limit
        pov_size_limits = self.calculate_volume_size_limits()
        mv_limits = DataFrame({'aum': aum_size_limit, 'pov': pov_size_limits})
        return np.min(mv_limits, axis=1).fillna(0.).map(Decimal)

    def calculate_volume_size_limits(self) -> pd.Series:
        """
        Calculate percentage-of-volume based position size limit.
        Ensures size of the position is below a fraction of volume.
        This fraction is configured using the pov_limit attribute.
        :return: the volume-based size limits
        """
        base_size_limits = self.volume * self.pov_limit
        quote_size_limits = self.prices * base_size_limits
        return quote_size_limits.fillna(Decimal('0'))

    def apply_size_limits(self, weights: pd.Series,
                          spending_limit: Decimal) -> pd.Series:
        """
        Applies position size limits to the weights.
        Ensures the returned weights would not cause exceeding position limits.
        :param weights: the initial weights
        :param spending_limit: the amount we can spend
        :return:
        """
        weight_limits = self.spending_limits / spending_limit
        weights, weight_limits = overlapping_labels(weights, weight_limits)
        weights = weights.where(weights < weight_limits, weight_limits)
        weights = weights[weights.notna() & weights.gt(0.)]
        return weights

    def apply_exchange_size_limits(self, weights: pd.Series,
                                   spending_limit: Decimal) -> pd.Series:
        spending_limit = float(spending_limit)
        weights = weights.map(float)
        if self.buy_order_type == 'market':
            min_market_funds = pd.Series(
                {product: float(info['min_market_funds'])
                 for product, info in self.market_info.items()})
            weights = limit_market_buy_size(spending_limit, weights,
                                            min_market_funds)
        else:
            base_min_sizes = pd.Series(
                {product: float(info['base_min_size'])
                 for product, info in self.market_info.items()})
            weights = limit_limit_buy_size(spending_limit, weights,
                                           self.bids.map(float),
                                           base_min_sizes)
        return weights.fillna(0.).map(Decimal)

    def limit_weights(self, target_weights: Series,
                      spending_limit: Decimal) -> Series:
        nil_weights = Series([], dtype=np.float64)
        filtered_weights = self.filter_weights(target_weights)
        if not len(filtered_weights):
            return nil_weights
        limited_weights = self.apply_size_limits(filtered_weights,
                                                 spending_limit)
        if not len(limited_weights):
            return nil_weights
        ranked_weights = limited_weights.sort_values(ascending=False)
        weights = self.apply_exchange_size_limits(ranked_weights,
                                                  spending_limit)
        logger.debug(weights)
        return weights

    def filter_weights(self, target_weights: pd.Series) -> pd.Series:
        overheated = filter(self.cool_down.cooling_down, target_weights.index)
        not_allowed = set(overheated) | set(self.blacklist)
        allowed = target_weights.index.difference(not_allowed)
        filtered_weights = target_weights.loc[allowed]
        return filtered_weights

    def queue_buys(self) -> None:
        """
        Queue buys for the top positive scoring assets.
        Total of price * size should be available_funds after taker_fee.
        Don't queue if would put position count over max_positions.
        Don't queue if would be below min_position_size.

        Don't queue if would put market above market_percentage_limit.
        Don't queue if would violate volatility cooldown.
        """
        budget = self.portfolio_available_funds
        spending_limit = budget / (Decimal('1') + self.taker_fee)
        weights = self.limit_weights(self.buy_weights, spending_limit)
        decimal_weights = weights.fillna(0.).map(Decimal)
        for market, weight in decimal_weights.iteritems():
            if market not in self.market_info:
                continue
            assert isinstance(market, str)
            self.counter.increment()
            previous_state = RootState(market=market,
                                       number=self.counter.monotonic_count)
            state_change = f'buy target {weight:.2f}'
            funds = Decimal(weight) * spending_limit
            if self.buy_order_type == 'limit' and market in self.bids:
                price = self.bids[market]
                size = funds / price
                buy = DesiredLimitBuy(price=price,
                                      size=size,
                                      market=market,
                                      previous_state=previous_state,
                                      state_change=state_change)
                logger.debug(buy)
                self.desired_limit_buys.append(buy)
            elif self.buy_order_type == 'market':
                buy = DesiredMarketBuy(funds=funds, market=market,
                                       previous_state=previous_state,
                                       state_change=state_change)
                logger.debug(buy)
                self.desired_market_buys.append(buy)
        return None

    def check_desired_market_buys(self) -> None:
        """
        Place GTC orders for desired buys and add to pending buys queue.

        Only place orders for markets that are online.
        Only place orders that are within exchange markets for market.
        """
        for buy in self.desired_market_buys:
            market = buy.market
            info = self.market_info[market]
            if info['status'] != 'online' or info['trading_disabled']:
                self.counter.decrement()
                continue
                # could probably use post_only in the order flags
            elif info['cancel_only'] or info['post_only']:
                self.counter.decrement()
                continue
            elif info['limit_only']:
                # could re-direct to limit order
                self.counter.decrement()
                continue
            funds = buy.funds.quantize(Decimal(info['quote_increment']),
                                       rounding='ROUND_DOWN')
            min_funds = Decimal(info['min_market_funds'])
            if funds < min_funds:
                self.counter.decrement()
                continue
            max_funds = Decimal(info['max_market_funds'])
            funds = min(funds, max_funds)
            order = self.exchange.retryable_market_order(market, side='buy',
                                                         funds=str(funds),
                                                         stp='cn')
            self.cool_down.bought(market)
            if 'id' not in order:
                logger.warning(order)
                self.counter.decrement()
                continue
            created_at = dateutil.parser.parse(order['created_at'])
            order_id = order['id']
            self.tracker.remember(order_id)
            pending = PendingMarketBuy(funds, market=market,
                                       order_id=order_id,
                                       created_at=created_at,
                                       previous_state=buy,
                                       state_change='order placed')
            logger.debug(pending)
            self.pending_market_buys.append(pending)
        # RESET DESIRED BUYS
        self.desired_market_buys = []

    def check_desired_limit_buys(self) -> None:
        """
        Place GTC orders for desired buys and add to pending buys queue.

        Only place orders for markets that are online.
        Only place orders that are within exchange limits for market.
        """
        next_generation: t.List[DesiredLimitBuy] = []
        for buy in self.desired_limit_buys:
            if not self.buy_weights.get(buy.market):
                self.counter.decrement()
                continue
            market = buy.market
            info = self.market_info[market]
            if info['status'] != 'online' or info['trading_disabled']:
                self.counter.decrement()
                continue
                # could probably use post_only in the order flags
            elif info['cancel_only']:
                self.counter.decrement()
                continue
            if buy.market not in self.bids:
                self.counter.decrement()
                continue
            bid = self.bids[buy.market]
            price = bid.quantize(Decimal(info['quote_increment']),
                                 rounding='ROUND_DOWN')
            size = buy.size.quantize(Decimal(info['base_increment']),
                                     rounding='ROUND_DOWN')
            min_size = Decimal(info['base_min_size'])
            if size < min_size:
                self.counter.decrement()
                continue
            max_size = Decimal(info['base_max_size'])
            size = min(size, max_size)
            post_only = self.post_only or info['post_only']
            tif = 'GTC' if post_only else self.buy_time_in_force
            order = self.exchange.retryable_limit_order(market, side='buy',
                                                        price=str(price),
                                                        size=str(size),
                                                        time_in_force=tif,
                                                        post_only=post_only,
                                                        stp='cn')
            self.cool_down.bought(market)
            if 'id' not in order:
                next_generation.append(buy)
                logger.warning(f"Error placing buy order {order}")
                continue  # This means there was a problem with the order
            created_at = dateutil.parser.parse(order['created_at'])
            order_id = order['id']
            self.tracker.remember(order_id)
            pending = PendingLimitBuy(price, size, market=market,
                                      order_id=order_id,
                                      created_at=created_at,
                                      previous_state=buy,
                                      state_change='order placed')
            logger.debug(pending)
            self.pending_limit_buys.append(pending)
        # RESET DESIRED BUYS
        self.desired_limit_buys = next_generation

    def check_pending_limit_buys(self) -> None:
        """
        Using "done" and "open" orders.
        Move done orders to active_positions.
        Cancel open orders that are older than age limit.
        If cancelling an order, add the filled_size to active_positions.
        """
        next_generation: t.List[PendingLimitBuy] = []  # Word to Bob
        for buy in self.pending_limit_buys:
            market_info = self.market_info[buy.market]
            if market_info['trading_disabled']:
                logger.info(f"Trading disabled: {buy}")
                next_generation.append(buy)
                continue
            order_id = buy.order_id
            if self.order_snapshot_time - buy.created_at < ORDER_WAIT_TIME:
                # buy was created during this iteration, nothing to do
                next_generation.append(buy)
                continue
            if order_id not in self.orders:
                # buy was canceled externally before being filled
                # candidate explanation is self-trade prevention
                # we don't do anything about this
                self.tracker.forget(order_id)
                self.counter.decrement()
                continue
            order = self.orders[order_id]
            status = order['status']
            # treat these the same?
            if status in {'open', 'pending', 'active'}:
                server_age = self.tick_time - buy.created_at
                time_limit_expired = server_age > self.buy_age_limit
                if time_limit_expired:
                    self.exchange.cancel_order(order_id)
                next_generation.append(buy)
                continue
            elif status == 'done':
                self.tracker.forget(order_id)
                size = Decimal(order['filled_size'])
                if size:
                    price = Decimal(order['executed_value']) / size
                    fee = Decimal(order['fill_fees'])
                    # place stop loss order
                    # place take profit order
                    # accounting for orders
                    position = ActivePosition(price, size, fee,
                                              market=buy.market,
                                              start=self.tick_time,
                                              previous_state=buy,
                                              state_change='order filled')
                    logger.debug(position)
                    self.active_positions.append(position)
                else:
                    self.counter.decrement()
                continue
            else:
                logger.warning(f"Unknown status {status}.")
                logger.debug(order)
                next_generation.append(buy)
        # RESET PENDING BUYS
        self.pending_limit_buys = next_generation

    def check_pending_market_buys(self) -> None:
        """
        Using "done" and "open" orders.
        Move done orders to active_positions.
        Cancel open orders that are older than age limit.
        If cancelling an order, add the filled_size to active_positions.
        """
        next_generation: t.List[PendingMarketBuy] = []  # Word to Bob
        for buy in self.pending_market_buys:
            market_info = self.market_info[buy.market]
            if market_info['trading_disabled']:
                next_generation.append(buy)
                continue
            order_id = buy.order_id
            if self.order_snapshot_time - buy.created_at < ORDER_WAIT_TIME:
                # buy was created during this iteration, nothing to do
                next_generation.append(buy)
                continue
            if order_id not in self.orders:
                # buy was canceled externally before being filled
                # candidate explanation is self-trade prevention
                # we don't do anything about this
                self.tracker.forget(order_id)
                self.counter.decrement()
                continue
            order = self.orders[order_id]
            status = order['status']
            if status in {'open', 'pending', 'active'}:
                next_generation.append(buy)
                continue
            elif status == 'done':
                self.tracker.forget(order_id)
                size = Decimal(order['filled_size'])
                if not size:
                    self.counter.decrement()
                    continue
                price = Decimal(order['executed_value']) / size
                fee = Decimal(order['fill_fees'])
                # place stop loss order
                # place take profit order
                # accounting for orders
                active_position = ActivePosition(price, size, fee,
                                                 market=buy.market,
                                                 start=self.tick_time,
                                                 previous_state=buy,
                                                 state_change='order filled')
                logger.debug(active_position)
                self.active_positions.append(active_position)
            else:
                logger.warning(f"Unknown status {status} for order {order}.")
                logger.debug(order)
                next_generation.append(buy)
                continue
        # RESET PENDING BUYS
        self.pending_market_buys = next_generation

    def compress_active_positions(self) -> None:
        accumulators: t.Dict[str, ActivePosition] = {}
        for position in self.active_positions:
            if position.market in accumulators:
                self.counter.decrement()
                accumulator = accumulators[position.market]
                both = accumulator.merge(position)
                logger.debug(f"merge: {position} + {accumulator} = {both}")
                accumulators[position.market] = both
            else:
                accumulators[position.market] = position
        self.active_positions = list(accumulators.values())

    def check_active_positions(self) -> None:
        """
        Adjust stop losses on active positions.
        Move stop loss triggered positions to desired limit sell.
        """
        next_generation: t.List[ActivePosition] = []
        for position in self.active_positions:
            market = position.market
            market_info = self.market_info[market]
            min_size = Decimal(market_info['base_min_size'])
            logger.debug(position)
            if position.size < min_size:
                next_generation.append(position)
                continue
            if market not in self.asks:
                next_generation.append(position)
                continue
            price = self.asks[market]
            stop_sale = self.stop_loss.trigger(price, position.price)
            if stop_sale:
                self.cool_down.sold(market)
                sell_fraction = Decimal(1)
            else:
                sell_fraction = self.sell_weights.get(market, Decimal(0))
            size_increment = Decimal(market_info['base_increment'])
            sell_size = compute_sell_size(position.size,
                                          sell_fraction,
                                          min_size,
                                          size_increment)
            remainder = position.size - sell_size
            if sell_size:
                if remainder:
                    self.counter.increment()
                sell_fraction = sell_size / position.size
                state_change = f'sell {sell_fraction:.3f}'
                if self.sell_order_type == 'limit':
                    sell = DesiredLimitSell(size=sell_size,
                                            market=market,
                                            previous_state=position,
                                            state_change=state_change,
                                            stop_sale=stop_sale)
                    logger.debug(sell)
                    self.desired_limit_sells.append(sell)
                else:
                    sell = DesiredMarketSell(size=sell_size,
                                             market=market,
                                             previous_state=position,
                                             state_change=state_change,
                                             stop_sale=stop_sale)
                    logger.debug(sell)
                    self.desired_market_sells.append(sell)
            if remainder == position.size:
                next_generation.append(position)
            elif remainder:
                next_position = position.drawdown_clone(remainder)
                next_generation.append(next_position)
            else:
                logger.debug(f"dropping position {position}")
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
                limit_sell = DesiredLimitSell(size=sell.size,
                                              market=sell.market,
                                              previous_state=sell,
                                              state_change=transition,
                                              stop_sale=sell.stop_sale)
                logger.debug(limit_sell)
                self.desired_limit_sells.append(limit_sell)
                continue
            exp = Decimal(self.market_info[sell.market]['base_increment'])
            size = sell.size.quantize(exp, rounding='ROUND_DOWN')
            order = self.exchange.retryable_market_order(sell.market,
                                                         side='sell',
                                                         size=str(size),
                                                         stp='dc')
            if 'id' not in order:
                logger.warning(f"Error placing order {order} {sell}")
                continue
            order_id = order['id']
            self.tracker.remember(order_id)
            created_at = dateutil.parser.parse(order['created_at'])
            pending_sell = PendingMarketSell(size=sell.size,
                                             market=sell.market,
                                             order_id=order_id,
                                             created_at=created_at,
                                             previous_state=sell,
                                             state_change='order created',
                                             stop_sale=sell.stop_sale)
            logger.debug(pending_sell)
            self.pending_market_sells.append(pending_sell)
        self.desired_market_sells = next_generation

    def check_pending_market_sells(self) -> None:
        """
        Monitor pending market sell orders.
        """
        next_generation: t.List[PendingMarketSell] = []
        for sell in self.pending_market_sells:
            order_id = sell.order_id
            if self.order_snapshot_time - sell.created_at < ORDER_WAIT_TIME:
                next_generation.append(sell)
                continue
            elif order_id not in self.orders:
                self.tracker.forget(order_id)
                desired_sell = DesiredMarketSell(market=sell.market,
                                                 size=sell.size,
                                                 previous_state=sell,
                                                 state_change='ext. canceled',
                                                 stop_sale=sell.stop_sale)
                logger.debug(desired_sell)
                self.desired_market_sells.append(desired_sell)
                continue
            order = self.orders[order_id]
            status = order['status']
            if status in {'pending', 'active', 'open'}:
                next_generation.append(sell)
                continue
            elif status == 'done':
                self.tracker.forget(order_id)
                size = sell.size
                filled_size = Decimal(order['filled_size'])
                self.counter.decrement()
                remainder = size - filled_size
                if filled_size:
                    self.counter.increment()
                    executed_value = Decimal(order['executed_value'])
                    executed_price = executed_value / filled_size
                    fee = Decimal(order['fill_fees'])
                    transition = 'fill' if not remainder else 'partial fill'
                    sold = Sold(market=sell.market, size=filled_size,
                                price=executed_price, fees=fee,
                                previous_state=sell,
                                state_change=transition)
                    logger.debug(sold)
                    self.sells.append(sold)
                if remainder:
                    self.counter.increment()
                    transition = 'ext. canceled'
                    desired_sell = DesiredMarketSell(market=sell.market,
                                                     size=remainder,
                                                     previous_state=sell,
                                                     state_change=transition,
                                                     stop_sale=sell.stop_sale)
                    logger.debug(desired_sell)
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
            market_info = self.market_info[sell.market]
            backing_off = self.sell_weights.get(sell.market, 0.) <= 0.
            size_too_small = sell.size < Decimal(market_info['base_min_size'])
            if (backing_off and not sell.stop_sale) or size_too_small:
                state_change = 'backed off' if backing_off else 'too small'
                position = ActivePosition(
                    market=sell.market,
                    size=sell.size,
                    price=sell.last_active_price(),
                    previous_state=sell,
                    fees=sell.cumulative_fees(),
                    start=self.tick_time,
                    state_change=state_change
                )
                self.active_positions.append(position)
                continue
            if market_info['trading_disabled']:
                next_generation.append(sell)
                continue
            quote_increment = Decimal(market_info['quote_increment'])
            if sell.market not in self.asks:
                next_generation.append(sell)
                continue
            price = self.asks[sell.market].quantize(quote_increment)
            post_only = market_info['post_only'] or self.post_only
            tif = 'GTC' if post_only else self.sell_time_in_force
            order = self.exchange.retryable_limit_order(product_id=sell.market,
                                                        side='sell',
                                                        price=str(price),
                                                        size=str(sell.size),
                                                        time_in_force=tif,
                                                        post_only=post_only,
                                                        stp='co')
            if 'id' not in order:
                # this means the market moved up
                if order.get('message') == 'Post only mode':
                    next_generation.append(sell)
                else:
                    position = ActivePosition(
                        market=sell.market,
                        size=sell.size,
                        price=sell.last_active_price(),
                        previous_state=sell,
                        fees=sell.cumulative_fees(),
                        start=self.tick_time,
                        state_change=order.get('message'),
                    )
                    self.active_positions.append(position)
                logger.debug(f"Error placing order {order} {sell}")
                continue
            order_id = order['id']
            self.tracker.remember(order_id)
            created_at = dateutil.parser.parse(order['created_at'])
            pending_sell = PendingLimitSell(price=price, size=sell.size,
                                            market=sell.market,
                                            order_id=order_id,
                                            created_at=created_at,
                                            previous_state=sell,
                                            state_change='order placed',
                                            stop_sale=sell.stop_sale)
            logger.debug(pending_sell)
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
            if self.order_snapshot_time - sell.created_at < ORDER_WAIT_TIME:
                # created during this generation, nothing to see here
                next_generation.append(sell)
                continue
            if order_id not in self.orders:
                self.tracker.forget(order_id)
                # External cancellation of pending order
                desired_sell = DesiredLimitSell(market=sell.market,
                                                size=sell.size,
                                                previous_state=sell,
                                                state_change='canceled',
                                                stop_sale=sell.stop_sale)
                logger.debug(desired_sell)
                self.desired_limit_sells.append(desired_sell)
                continue
            order = self.orders[order_id]
            status = order['status']
            if status in {'active', 'pending', 'open'}:
                server_age = self.tick_time - sell.created_at
                time_limit_expired = server_age > self.sell_age_limit
                if time_limit_expired:
                    self.exchange.cancel_order(order_id)
                next_generation.append(sell)
                continue
            elif status == 'done':
                self.tracker.forget(order_id)
                executed_value = Decimal(order['executed_value'])
                filled_size = Decimal(order['filled_size'])
                self.counter.decrement()
                remainder = sell.size - filled_size
                if filled_size:
                    self.counter.increment()
                    state_change = 'partial fill' if remainder else 'filled'
                    executed_price = executed_value / filled_size
                    sold = Sold(price=executed_price, size=filled_size,
                                fees=Decimal(order['fill_fees']),
                                market=sell.market,
                                previous_state=sell,
                                state_change=state_change,
                                )
                    logger.debug(sold)
                    self.sells.append(sold)
                if remainder:
                    self.counter.increment()
                    desired_sell = DesiredLimitSell(market=sell.market,
                                                    size=remainder,
                                                    previous_state=sell,
                                                    state_change='canceled',
                                                    stop_sale=sell.stop_sale)
                    logger.debug(desired_sell)
                    self.desired_limit_sells.append(desired_sell)
            else:
                logger.warning(f"Unknown status: {status}")
                logger.debug(order)
                next_generation.append(sell)
                continue
        self.pending_limit_sells = next_generation

    def check_sold(self) -> None:
        for _ in self.sells:
            self.counter.decrement()
        self.sells = []

    def set_tick_variables(self) -> None:
        self.set_portfolio_available_funds()
        candles = self.candles_src.compute()
        volume = self.volume_indicator.compute(candles)
        self.volume = volume.fillna(0.).map(Decimal)
        prices = self.price_indicator.compute(candles)
        self.prices = safely_decimalize(prices)
        self.order_snapshot_time, self.orders = self.tracker.barrier_snapshot()
        self.tick_time, last_tick_time = get_server_time(), self.tick_time
        self.cool_down.set_tick(self.tick_time)
        buy_targets = self.buy_indicator.compute(candles)
        sell_targets = self.sell_indicator.compute(candles)
        if last_tick_time:
            buy_horizon_seconds = self.buy_horizon.total_seconds()
            sell_horizon_seconds = self.sell_horizon.total_seconds()
            last_tick_duration = self.tick_time - last_tick_time
            duration = last_tick_duration.total_seconds()
            buy_target_periods = np.floor(buy_horizon_seconds / duration)
            sell_target_periods = np.floor(sell_horizon_seconds / duration)
        else:
            buy_target_periods = pd.Series([], dtype=np.float64)
            sell_target_periods = pd.Series([], dtype=np.float64)
        self.buy_weights = adjust_spending_target(buy_targets,
                                                  buy_target_periods)
        self.sell_weights = adjust_spending_target(sell_targets,
                                                   sell_target_periods)
        # these are down here so they're computed last
        # use bid/ask in a buy/sell context and prices everywhere else
        bid_ask = self.bid_ask_indicator.compute()
        bids = bid_ask['bid']
        self.bids = bids.map(Decimal).where(bids.notna(), pd.NA)
        asks = bid_ask['ask']
        self.asks = asks.map(Decimal).where(asks.notna(), pd.NA)

    def set_market_info(self) -> None:
        self.market_info = {product['id']: product for product in
                            self.exchange.get_products()}

    def set_fee(self) -> None:
        fee_info = self.exchange.get_fees()
        self.taker_fee = Decimal(fee_info['taker_fee_rate'])
        self.maker_fee = Decimal(fee_info['maker_fee_rate'])

    def set_portfolio_available_funds(self) -> None:
        quote_account = self.exchange.get_account(self.quote_account_id)
        self.portfolio_available_funds = Decimal(quote_account['available'])

    def liquidate(self) -> None:
        for account in self.exchange.get_accounts():
            if account['currency'] == self.quote:
                continue
            if not Decimal(account['available']):
                continue
            market = f"{account['currency']}-{self.quote}"
            self.exchange.place_market_order(market,
                                             side='sell',
                                             size=account['available'])

    def shutdown(self) -> None:
        logger.info(f"Shutting down...")
        self.exchange.cancel_all()
        if self.liquidate_on_shutdown:
            self.liquidate()
        self.stop = True
        self.tracker.stop()

    def initialize(self) -> None:
        n = 15
        logger.info(f"Waiting {n} seconds to start trading...")
        time.sleep(n)
        self.exchange.cancel_all()
        self.initialize_active_positions()
        self.initialized = True

    def initialize_active_positions(self) -> None:
        positions: t.List[ActivePosition] = []
        for account in self.exchange.get_accounts():
            if account['currency'] == self.quote:
                continue
            market = f"{account['currency']}-{self.quote}"
            if market not in self.market_info:
                continue
            # This could allow ring-fencing
            if market in self.blacklist:
                continue
            balance = Decimal(account['balance'])
            if balance < Decimal(self.market_info[market]['base_min_size']):
                continue
            if market not in self.prices:
                continue
            price = self.prices[market]
            self.counter.increment()
            tail = Download(self.counter.monotonic_count, market=market)
            position = ActivePosition(price, balance, fees=Decimal('0'),
                                      market=market, start=self.tick_time,
                                      previous_state=tail,
                                      state_change='downloaded')
            positions.append(position)
        self.active_positions = positions

    def run(self) -> t.NoReturn:
        self.set_market_info()
        self.set_fee()
        while not self.stop:
            iteration_start = time.time()
            self.set_tick_variables()
            if not self.initialized:
                self.initialize()
            self.manage_positions()
            tick_duration = time.time() - iteration_start
            logger.info(f"Tick took {tick_duration :.1f}s")
            wait = max(0., self.min_tick_time - tick_duration)
            time.sleep(wait)

    def manage_positions(self):
        start = time.time()
        self.check_sold()
        self.check_pending_market_sells()
        self.check_pending_limit_sells()
        self.check_pending_limit_buys()
        self.check_pending_market_buys()

        self.queue_buys()
        self.check_desired_limit_buys()
        self.check_desired_market_buys()
        self.compress_active_positions()
        self.check_active_positions()
        self.check_desired_market_sells()
        self.check_desired_limit_sells()
        logger.info(f"Position check took {time.time() - start:.2f}s")


__all__ = ['PortfolioManager']
