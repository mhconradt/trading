import time
import typing as t
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from threading import Lock

import cbpro
import dateutil.parser

from brain.order_tracker import OrderTracker


@dataclass
class OrderState:
    id: str
    status: str
    size: Decimal
    price: Decimal
    executed_value: Decimal = Decimal(0)
    filled_size: Decimal = Decimal(0)
    fill_fees: Decimal = Decimal(0)
    done_reason: t.Optional[str] = None

    def as_coinbase(self) -> dict:
        return {
            'id': self.id,
            'status': self.status,
            'size': str(self.size),
            'price': str(self.price),
            'executed_value': str(self.executed_value),
            'filled_size': str(self.filled_size),
            'done_reason': self.done_reason,
            'fill_fees': str(self.fill_fees),
        }


class OrderTrackerClient(cbpro.WebsocketClient):
    def __init__(self, products: t.List[str], api_passphrase: str,
                 api_secret: str,
                 api_key: str):
        super().__init__(products=products,
                         channels=['user', 'heartbeat'],
                         auth=True,
                         api_key=api_key,
                         api_passphrase=api_passphrase,
                         api_secret=api_secret)
        self._lock = Lock()
        self._orders = {}
        self._timestamp = ''

    def forget(self, order_id: str) -> None:
        with self._lock:
            if order_id in self._orders:
                self._orders.pop(order_id)

    def snapshot(self) -> t.Tuple[str, dict]:
        with self._lock:
            # note that this makes a copy
            snapshot = {s.id: s.as_coinbase() for s in self._orders.values()}
            return self._timestamp, snapshot

    def on_message(self, msg: dict) -> None:
        if msg['type'] == 'heartbeat' or msg['type'] == 'subscriptions':
            return None
        if 'order_id' in msg:
            order_id = msg['order_id']
        else:
            maker, taker = msg['maker_order_id'], msg['taker_order_id']
            order_id = maker if maker in self._orders else taker
        prev_state = self._orders.get(order_id)
        if msg['type'] == 'received':
            state = OrderState(id=order_id, status='pending',
                               size=Decimal(msg['size']),
                               price=Decimal(msg['price']))
            with self._lock:
                self._orders[order_id] = state
                self._timestamp = max(self._timestamp, msg['time'])
        elif msg['type'] == 'open' and order_id in self._orders:
            state = OrderState(id=prev_state.id,
                               status='open',
                               size=prev_state.size,
                               price=prev_state.price)
            with self._lock:
                self._orders[order_id] = state
                self._timestamp = max(self._timestamp, msg['time'])
        elif msg['type'] == 'match' and order_id in self._orders:
            executed_value_delta = Decimal(msg['size']) * Decimal(msg['price'])
            filled_size_delta = Decimal(msg['size'])
            executed_value = prev_state.executed_value + executed_value_delta
            filled_size = prev_state.filled_size + filled_size_delta
            fee_rate = Decimal(msg.get('maker_fee_rate',
                                       msg.get('taker_fee_rate')))
            fee_delta = executed_value_delta * fee_rate
            fill_fees = prev_state.fill_fees + fee_delta
            state = OrderState(id=order_id,
                               status=prev_state.status,
                               size=prev_state.size,
                               price=prev_state.price,
                               executed_value=executed_value,
                               filled_size=filled_size,
                               fill_fees=fill_fees)
            with self._lock:
                self._orders[order_id] = state
                self._timestamp = max(self._timestamp, msg['time'])
        elif msg['type'] == 'change' and order_id in self._orders:
            state = OrderState(id=order_id,
                               status=prev_state.status,
                               size=Decimal(msg['new_size']),
                               price=prev_state.price,
                               executed_value=prev_state.executed_value,
                               filled_size=prev_state.filled_size,
                               fill_fees=prev_state.fill_fees,
                               )
            with self._lock:
                self._orders[order_id] = state
                self._timestamp = max(self._timestamp, msg['time'])
        elif msg['type'] == 'done' and order_id in self._orders:
            done_reason = msg['reason']
            state = OrderState(id=order_id,
                               status='done',
                               done_reason=done_reason,
                               size=prev_state.size,
                               price=prev_state.price,
                               executed_value=prev_state.executed_value,
                               filled_size=prev_state.filled_size,
                               fill_fees=prev_state.fill_fees)
            with self._lock:
                self._orders[order_id] = state
                self._timestamp = max(self._timestamp, msg['time'])
        else:
            pass


class AsyncOrderTracker(OrderTracker):
    def __init__(self, client: OrderTrackerClient):
        self.client = client
        self.watchlist = set()

    def remember(self, order_id: str) -> None:
        self.watchlist.add(order_id)

    def barrier_snapshot(self) -> t.Tuple[datetime, dict]:
        if self.client.stop:
            raise ValueError()
        timestamp, snapshot = self.client.snapshot()
        for order_id in snapshot:
            if order_id not in self.watchlist:
                self.client.forget(order_id)
            snapshot.pop(order_id)
        return dateutil.parser.parse(timestamp), snapshot

    def snapshot(self) -> dict:
        _, snapshot = self.barrier_snapshot()
        return snapshot

    def forget(self, order_id: str) -> None:
        if order_id in self.watchlist:
            self.watchlist.remove(order_id)
        self.client.forget(order_id)


def main():
    products = [product['id'] for product in
                cbpro.PublicClient().get_products()
                if product['quote_currency'] == 'USD']
    ws_client = OrderTrackerClient(products=products,
                                   api_passphrase='omhj8xopyq',
                                   api_secret='93X34sXJdBToewnMg4XnEKlt7AZ/UR9aZLN/APAEWSmnaNifkvFmze0k5/W8jDNgvtcCH3syu+yaOniv0AHd0A==',
                                   api_key='7a03bd73cb494b04d27501a2b582e8c6')
    ws_client.start()
    tracker = AsyncOrderTracker(ws_client)
    while True:
        time.sleep(15)
        timestamp, snapshot = tracker.barrier_snapshot()
        print(sum(
            map(lambda o: Decimal(o['executed_value']), snapshot.values())))
        print(timestamp)


if __name__ == '__main__':
    main()
