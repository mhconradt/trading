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
                            PendingCancelSell)
from brain.order_tracker import SyncCoinbaseOrderTracker


# TODO: Self-trade prevention
# TODO: Limit # of orders (per product)


class PortfolioManager:
    def __init__(self, client: AuthenticatedClient):
        self.client = client
        self.tracker = SyncCoinbaseOrderTracker(client)
        self.min_position_size = Decimal('10')
        self.max_positions = 10_000
        self.position_count = 0

        self.buy_age_limit = timedelta(minutes=1)
        self.sell_age_limit = timedelta(minutes=1)

        # STATES
        self.desired_buys: deque[DesiredLimitBuy] = deque()
        self.pending_buys: deque[PendingLimitBuy] = deque()
        self.pending_cancel_buys: t.List[PendingCancelBuy] = []
        self.active_positions: t.List[ActivePosition] = []
        self.desired_sells: t.List[DesiredLimitSell] = []
        self.pending_sells: t.List[PendingLimitSell] = []
        self.pending_cancel_sells: t.List[PendingCancelSell] = []
        self.sells: deque[Sold] = deque()

    def queue_buys(self, scores: Series, prices: Series,
                   spending_limit: Decimal) -> None:
        """
        Queue buys for the top positive scoring assets.
        Total of price * size should be available_funds after taker_fee.
        Don't queue if would put position count over max_positions.
        Don't queue if would be below min_position_size.

        Don't queue if would put market above market_percentage_limit.
        Don't queue if would violate volatility cooldown.
        """
        if spending_limit < self.min_position_size:
            return
        if self.position_count == self.max_positions:
            return
        positive_scores = scores[scores.notna() & scores.gt(0.)]
        ranked_scores = positive_scores.sort_values(ascending=False)
        cumulative_normalized_scores = ranked_scores / ranked_scores.cumsum()
        hypothetical_sizes = cumulative_normalized_scores * spending_limit
        hypothetical_sizes_ok = hypothetical_sizes >= self.min_position_size
        min_position_size_limit = np.arange(hypothetical_sizes_ok).max() + 1
        position_count_limit = self.max_positions - self.position_count
        limit = min(min_position_size_limit, position_count_limit)
        if not limit:
            return
        final_scores = ranked_scores.iloc[:limit]
        weights = final_scores / final_scores.sum()
        for market, weight in weights.iteritems():
            price = Decimal.from_float(prices.loc[market])
            size = Decimal.from_float(weight) * spending_limit / price
            buy = DesiredLimitBuy(price=price,
                                  size=size,
                                  market=market, history=[])
            self.desired_buys.append(buy)
            self.position_count += 1
        return None

    def check_desired_buys(self, market_info: t.Dict[str, dict]) -> None:
        """
        Place GTC orders for desired buys and add to pending buys queue.

        Only place orders for markets that are online.
        Only place orders that are within exchange limits for market.

        Percentage-of-volume trading at some point?
        """
        for buy in self.desired_buys:
            market = buy.market
            info = market_info[market]
            if info['status'] != 'online':
                continue
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
            self.tracker.track(order_id)
            pending = PendingLimitBuy(price, size, market=market,
                                      order_id=order_id,
                                      created_at=created_at, history=[buy])
            self.pending_buys.append(pending)
        # RESET DESIRED BUYS
        self.desired_buys = []

    def check_pending_buys(self, order_snapshot: t.Dict[str, dict],
                           server_time: datetime) -> None:
        """
        Using "done" and "open" orders.
        Move done orders to active_positions.
        Cancel open orders that are older than age limit.
        If cancelling an order, add the filled_size to active_positions.
        """
        next_generation = []  # Word to Bob
        for pending_buy in self.pending_buys:
            order_id = pending_buy.order_id
            if pending_buy.created_at > server_time:
                # buy was created during this iteration, nothing to do
                next_generation.append(pending_buy)
                continue
            if order_id not in order_snapshot:
                # buy was canceled externally before being filled
                # candidate explanation is self-trade prevention
                self.position_count -= 1
                continue
            order = order_snapshot[order_id]
            status = order['status']
            # treat these the same?
            if status in {'open', 'pending', 'active'}:
                server_age = pending_buy.created_at - server_time
                time_limit_expired = server_age > self.buy_age_limit
                if time_limit_expired:
                    self.client.cancel_order(order_id)
                    history = [pending_buy, *pending_buy.history]
                    cancel_buy = PendingCancelBuy(
                        market=pending_buy.market,
                        price=pending_buy.price,
                        size=pending_buy.size,
                        order_id=order_id,
                        created_at=pending_buy.created_at,
                        history=history
                    )
                    self.pending_cancel_buys.append(cancel_buy)
                else:
                    next_generation.append(pending_buy)
            elif status == 'done':
                self.tracker.untrack(order_id)
                size = Decimal(order['filled_size'])
                price = Decimal(order['executed_value']) / size
                fees = Decimal(order['fee'])
                history = [pending_buy,
                           *pending_buy.history]
                # place stop loss order
                # place take profit order
                # accounting for orders
                active_position = ActivePosition(price, size, fees,
                                                 market=pending_buy.market,
                                                 start=server_time,
                                                 history=history)
                self.active_positions.append(active_position)
            else:
                raise ValueError(f"Unknown order status {status}")
        # RESET PENDING BUYS
        self.pending_buys = next_generation

    def check_active_positions(self, prices: Series, fee: Decimal) -> None:
        """
        Adjust stop losses on active positions.
        Move stop loss triggered positions to desired limit sell.

        :param prices: Pandas Series where index is product and key is price.
        :param fee: Fee as fraction, i.e. 0.005
        """
        next_generation = []
        for position in self.active_positions:
            price = Decimal.from_float(prices.loc[position.market])
            if position.sell(price, fee):
                history = [position, *position.history]
                sell = DesiredLimitSell(price, position.size,
                                        market=position.market,
                                        history=history)
                self.desired_sells.append(sell)
            else:
                next_generation.append(position)
        self.active_positions = next_generation

    def check_desired_sells(self) -> None:
        """
        Place limit sell orders for desired sells.
        """
        for sell in self.desired_sells:
            order = self.client.place_limit_order(sell.market, side='sell',
                                                  price=str(sell.price),
                                                  size=str(sell.size),
                                                  time_in_force='GTC',
                                                  stp='co')
            if 'id' not in order:
                continue
            order_id = order['id']
            self.tracker.track(order_id)
            created_at = dateutil.parser.parse(order['created_at'])
            history = [sell, *sell.history]
            pending_sell = PendingLimitSell(price=sell.price, size=sell.size,
                                            market=sell.market,
                                            order_id=order_id,
                                            created_at=created_at,
                                            history=history)
            self.pending_sells.append(pending_sell)

    def check_pending_sells(self, order_snapshot: t.Dict[str, dict],
                            server_time: datetime) -> None:
        """
        Using "done" and "open" orders.
        Move positions whose orders are "done" to sold positions.
        Move positions whose orders are open to pending sells.
        """
        next_generation = []
        for pending_sell in self.pending_sells:
            order_id = pending_sell.order_id
            if pending_sell.created_at > server_time:
                # created during this generation, nothing to see here
                next_generation.append(pending_sell)
                continue
            elif order_id not in order_snapshot:
                # TODO: Pending sell externally cancelled
                # For now you just end up with a little bit more of what you had
                self.position_count -= 1
                continue
            order = order_snapshot[order_id]
            status = order['status']
            if status in {'active', 'pending', 'open'}:
                server_age = server_time - pending_sell.created_at
                time_limit_expired = server_age > self.sell_age_limit
                if time_limit_expired:
                    self.client.cancel_order(order_id)
                    cancellation = PendingCancelSell(
                        market=pending_sell.market,
                        price=pending_sell.price,
                        size=pending_sell.size,
                        order_id=order_id,
                        created_at=pending_sell.created_at,
                        history=[pending_sell,
                                 *pending_sell.history])
                    self.pending_cancel_sells.append(cancellation)
                else:
                    next_generation.append(pending_sell)
            elif status == 'done':
                self.tracker.untrack(order_id)
                # external cancellation without being filled
                executed_value = Decimal(order['executed_value'])
                size = Decimal(order['filled_size'])
                executed_price = executed_value / size
                sell = Sold(price=executed_price, size=size,
                            fees=Decimal(order['fee']),
                            market=pending_sell.market,
                            history=[pending_sell, *pending_sell.history])
                self.sells.append(sell)
            else:
                raise ValueError(f"Unknown status {status}")
        self.pending_sells = next_generation

    def check_cancel_buys(self, order_snapshot: t.Dict[str, dict],
                          server_time: datetime) -> None:
        next_generation: t.List[PendingCancelBuy] = []
        for cancellation in self.pending_cancel_buys:
            order_id = cancellation.order_id
            if order_id not in order_snapshot:
                self.tracker.untrack(order_id)
                self.position_count -= 1  # canceled without fill
            order = order_snapshot[order_id]
            status = order['status']
            if status == {'active', 'pending', 'open'}:
                next_generation.append(cancellation)
                continue
            elif status == 'done':
                self.tracker.untrack(order_id)
                executed_value = Decimal(order['executed_value'])
                filled_size = Decimal(order['filled_size'])
                executed_price = executed_value / filled_size
                position = ActivePosition(market=cancellation.market,
                                          price=executed_price,
                                          size=filled_size,
                                          fees=Decimal(order['fee']),
                                          start=server_time,
                                          history=[cancellation,
                                                   *cancellation.history])
                self.active_positions.append(position)
            else:
                print(order)
                raise ValueError(f"Unknown status {status}")
        self.pending_cancel_buys = next_generation

    def check_cancel_sells(self, order_snapshot: t.Dict[str, dict],
                           prices: Series) -> None:
        next_generation: t.List[PendingCancelSell] = []
        for cancellation in self.pending_cancel_sells:
            order_id = cancellation.order_id
            order = order_snapshot.get(order_id, None)
            if order is None or order['status'] == 'done':
                self.tracker.untrack(order_id)
                e_v = Decimal(order['executed_value'] if order else '0')
                filled_size = Decimal(order['filled_size'] if order else '0')
                history = [cancellation, *cancellation.history]
                remainder = cancellation.size - filled_size
                if remainder:
                    buy = DesiredLimitBuy(prices.loc[cancellation.market],
                                          remainder, cancellation.market,
                                          history=history)
                    self.desired_buys.append(buy)
                    self.position_count += 1  # fork
                if order is None or not filled_size:
                    continue
                executed_price = e_v / filled_size
                sell = Sold(price=executed_price, size=filled_size,
                            fees=Decimal(order['fee']),
                            market=cancellation.market,
                            history=history)
                self.sells.append(sell)
                continue
            elif order['status'] in {'active', 'pending', 'open'}:
                next_generation.append(cancellation)
        self.pending_cancel_sells = next_generation

    def check_sold(self) -> None:
        """
        Record final information about each position.
        """
        while self.sells:
            sell = self.sells.popleft()
            for position in sell.history:
                if isinstance(position, ActivePosition):
                    gain = (sell.price - position.price) * sell.size
                    print(f"{sell.market}: ${gain}")
                    break
            self.position_count -= 1

    def run(self, get_prices, get_scores) -> t.NoReturn:
        accounts = self.client.get_accounts()
        usd_account_id = [account['id'] for account in accounts if
                          account['currency'] == 'USD'][0]
        while True:
            usd_account = self.client.get_account(usd_account_id)
            available_funds = Decimal(usd_account['available'])
            prices = get_prices()
            scores = get_scores()
            product_infos = {product['id']: product for product in
                             self.client.get_products()}
            snapshot = self.tracker.snapshot()
            fee_info = self.client._send_message('GET', '/fees')
            fee = Decimal(fee_info['taker_fee'])
            time = dateutil.parser.parse(self.client.get_time()['iso'])
            self.check_sold()
            self.check_cancel_sells(snapshot, prices)
            self.check_pending_sells(snapshot, server_time=time)
            self.check_desired_sells()
            self.check_active_positions(prices, fee)
            self.check_cancel_buys(snapshot, time)
            self.check_pending_buys(snapshot, server_time=time)
            self.check_desired_buys(product_infos)
            self.queue_buys(prices, scores, available_funds)
