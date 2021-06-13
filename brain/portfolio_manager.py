from collections import defaultdict, deque
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


class PortfolioManager:
    def __init__(self, client: AuthenticatedClient):
        self.client = client
        self.min_position_size = Decimal('10')
        self.max_positions = 500
        self.current_positions = 0

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
        if self.current_positions == self.max_positions:
            return
        positive_scores = scores[scores.notna() & scores.gt(0.)]
        ranked_scores = positive_scores.sort_values(ascending=False)
        cumulative_normalized_scores = ranked_scores / ranked_scores.cumsum()
        hypothetical_sizes = cumulative_normalized_scores * spending_limit
        hypothetical_sizes_ok = hypothetical_sizes >= self.min_position_size
        min_position_size_limit = np.arange(hypothetical_sizes_ok).max() + 1
        position_count_limit = self.max_positions - self.current_positions
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
            self.current_positions += 1
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
                                                  time_in_force='GTC')
            if 'id' not in order:
                continue
            created_at = dateutil.parser.parse(order['created_at'])
            pending = PendingLimitBuy(price, size, market=market,
                                      order_id=order['id'],
                                      created_at=created_at, history=[buy])
            self.pending_buys.append(pending)
        # RESET DESIRED BUYS
        self.desired_buys = []

    def check_pending_buys(self, open_orders: t.Dict[str, dict],
                           done_orders: t.Dict[str, dict],
                           server_time: datetime) -> None:
        """
        Using "done" and "open" orders.
        Move done orders to active_positions.
        Cancel open orders that are older than age limit.
        If cancelling an order, add the filled_size to active_positions.
        """
        next_generation = []  # Word to Bob
        for pending_buy in self.pending_buys:
            if pending_buy.order_id in open_orders:
                server_age = pending_buy.created_at - server_time
                time_limit_expired = server_age > self.buy_age_limit
                if time_limit_expired:
                    self.client.cancel_order(pending_buy.order_id)
                    history = [pending_buy, *pending_buy.history]
                    cancel_buy = PendingCancelBuy(
                        market=pending_buy.market,
                        price=pending_buy.price,
                        size=pending_buy.size,
                        order_id=pending_buy.order_id,
                        created_at=pending_buy.created_at,
                        history=history
                    )
                    self.pending_cancel_buys.append(cancel_buy)
                else:
                    next_generation.append(pending_buy)
            elif pending_buy.order_id in done_orders:
                order = done_orders[pending_buy.order_id]
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
                next_generation.append(pending_buy)
                continue
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
                                                  time_in_force='GTC')
            if 'id' not in order:
                continue
            order_id = order['id']
            created_at = dateutil.parser.parse(order['created_at'])
            history = [sell, *sell.history]
            pending_sell = PendingLimitSell(price=sell.price, size=sell.size,
                                            market=sell.market,
                                            order_id=order_id,
                                            created_at=created_at,
                                            history=history)
            self.pending_sells.append(pending_sell)

    def check_pending_sells(self, open_orders: t.Dict[str, dict],
                            done_orders: t.Dict[str, dict],
                            server_time: datetime) -> None:
        """
        Using "done" and "open" orders.
        Move positions whose orders are "done" to sold positions.
        Move positions whose orders are open to pending sells.
        """
        next_generation = []
        for pending_sell in self.pending_sells:
            oid = pending_sell.order_id
            if oid in open_orders:
                server_age = server_time - pending_sell.created_at
                time_limit_expired = server_age > self.sell_age_limit
                if time_limit_expired:
                    self.client.cancel_order(oid)  # TODO: Partial fill.
                    cancellation = PendingCancelSell(
                        market=pending_sell.market,
                        price=pending_sell.price,
                        size=pending_sell.size,
                        order_id=oid,
                        created_at=pending_sell.created_at,
                        history=[pending_sell,
                                 *pending_sell.history])
                    self.pending_cancel_sells.append(cancellation)
                else:
                    next_generation.append(pending_sell)
            elif oid in done_orders:
                order = done_orders[oid]
                executed_value = Decimal(order['executed_value'])
                size = Decimal(order['filled_size'])
                executed_price = executed_value / size
                sell = Sold(price=executed_price, size=size,
                            fees=Decimal(order['fee']),
                            market=pending_sell.market,
                            history=[pending_sell, *pending_sell.history])
                self.sells.append(sell)
            else:
                next_generation.append(pending_sell)
        self.pending_sells = next_generation

    def check_cancel_buys(self, open_orders: t.Dict[str, dict],
                          done_orders: t.Dict[str, dict],
                          server_time: datetime) -> None:
        next_generation: t.List[PendingCancelBuy] = []
        for cancellation in self.pending_cancel_buys:
            order_id = cancellation.order_id
            if order_id in open_orders:
                next_generation.append(cancellation)
                continue
            elif order_id in done_orders:
                order = done_orders[order_id]
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
                self.current_positions -= 1  # canceled without fill
        self.pending_cancel_buys = next_generation

    def check_cancel_sells(self, open_orders: t.Dict[str, dict],
                           done_orders: t.Dict[str, dict],
                           prices: Series) -> None:
        next_generation: t.List[PendingCancelSell] = []
        for cancellation in self.pending_cancel_sells:
            order_id = cancellation.order_id
            if order_id in open_orders:
                next_generation.append(cancellation)
                continue
            elif order_id in done_orders:
                order = done_orders[order_id]
                executed_value = Decimal(order['executed_value'])
                filled_size = Decimal(order['filled_size'])
                history = [cancellation, *cancellation.history]
                remainder = cancellation.size - filled_size
                if remainder:
                    buy = DesiredLimitBuy(prices.loc[cancellation.market],
                                          remainder, cancellation.market,
                                          history=history)
                    self.desired_buys.append(buy)
                    self.current_positions += 1  # fork
                if not filled_size:
                    continue
                executed_price = executed_value / filled_size
                sell = Sold(price=executed_price, size=filled_size,
                            fees=Decimal(order['fee']),
                            market=cancellation.market,
                            history=history)
                self.sells.append(sell)
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
            self.current_positions -= 1

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
            # TODO: Limit these orders
            orders = self.client.get_orders(status=['done', 'open'])
            open_orders = {o['id']: o for o in orders if o['status'] == 'open'}
            done_orders = {o['id']: o for o in orders if o['status'] == 'done'}
            fee_info = self.client._send_message('GET', '/fees')
            fee = Decimal(fee_info['taker_fee'])
            time = dateutil.parser.parse(self.client.get_time()['iso'])
            self.check_sold()
            self.check_cancel_sells(open_orders, done_orders, prices)
            self.check_pending_sells(open_orders=open_orders,
                                     done_orders=done_orders,
                                     server_time=time)
            self.check_desired_sells()
            self.check_active_positions(prices, fee)
            self.check_cancel_buys(open_orders, done_orders, time)
            self.check_pending_buys(open_orders=open_orders,
                                    done_orders=done_orders,
                                    server_time=time)
            self.check_desired_buys(product_infos)
            self.queue_buys(prices, scores, available_funds)
