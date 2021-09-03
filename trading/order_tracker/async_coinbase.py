import logging
import time
import typing as t
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from threading import Lock

import cbpro
import dateutil.parser

from trading.coinbase.helper import get_server_time
from trading.coinbase.websocket_client import WebsocketClient
from trading.order_tracker.base import OrderTracker

logger = logging.getLogger(__name__)


# Simulate the Coinbase API.
# NOTE: Not tested for use with market orders.


@dataclass
class LimitOrderState:
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

    @staticmethod
    def from_received(msg: dict) -> "LimitOrderState":
        order_id = msg['order_id']
        state = LimitOrderState(id=order_id, status='pending',
                                size=Decimal(msg['size']),
                                price=Decimal(msg['price']))
        return state

    def done(self, msg: dict) -> "LimitOrderState":
        order_id = msg['order_id']
        done_reason = msg['reason']
        state = LimitOrderState(id=order_id,
                                status='done',
                                done_reason=done_reason,
                                size=self.size,
                                price=self.price,
                                executed_value=self.executed_value,
                                filled_size=self.filled_size,
                                fill_fees=self.fill_fees)
        return state

    def change(self, msg: dict) -> "LimitOrderState":
        order_id = msg['order_id']
        state = LimitOrderState(id=order_id,
                                status=self.status,
                                size=Decimal(msg['new_size']),
                                price=self.price,
                                executed_value=self.executed_value,
                                filled_size=self.filled_size,
                                fill_fees=self.fill_fees,
                                )
        return state

    def match(self, msg: dict) -> "LimitOrderState":
        order_id = self.id
        executed_value_delta = Decimal(msg['size']) * Decimal(msg['price'])
        filled_size_delta = Decimal(msg['size'])
        executed_value = self.executed_value + executed_value_delta
        filled_size = self.filled_size + filled_size_delta
        fee_rate = Decimal(msg.get('maker_fee_rate',
                                   msg.get('taker_fee_rate')))
        fee_delta = executed_value_delta * fee_rate
        fill_fees = self.fill_fees + fee_delta
        state = LimitOrderState(id=order_id,
                                status=self.status,
                                size=self.size,
                                price=self.price,
                                executed_value=executed_value,
                                filled_size=filled_size,
                                fill_fees=fill_fees)
        return state

    def open(self, _msg: dict) -> "LimitOrderState":
        state = LimitOrderState(id=self.id,
                                status='open',
                                size=self.size,
                                price=self.price)
        return state


class OrderTrackerClient(WebsocketClient):
    def __init__(self, products: t.List[str], api_passphrase: str,
                 api_secret: str,
                 api_key: str):
        # only subscribe to heartbeat for one channel
        channels = [{'name': 'user', 'product_ids': products},
                    {'name': 'heartbeat', 'product_ids': products[:1]}]
        super().__init__(products=[],
                         channels=channels,
                         auth=True,
                         api_key=api_key,
                         api_passphrase=api_passphrase,
                         api_secret=api_secret)
        self._lock = Lock()
        self._orders: t.Dict[str, LimitOrderState] = {}
        self._timestamp: datetime = get_server_time()

    def forget(self, order_id: str) -> None:
        with self._lock:
            if order_id in self._orders:
                self._orders.pop(order_id)

    def snapshot(self) -> t.Tuple[datetime, dict]:
        with self._lock:
            # note with care that this makes a copy
            snapshot = {s.id: s.as_coinbase() for s in self._orders.values()}
            return self._timestamp, snapshot

    def on_message(self, msg: dict) -> None:
        msg_type = msg['type']
        if msg_type == 'subscriptions' or msg_type == 'heartbeat':
            return None
        with self._lock:
            timestamp = dateutil.parser.parse(msg['time'])
            order_id = self.get_order_id(msg)
            prev_state = self._orders.get(order_id)
            if msg_type == 'received':
                state = LimitOrderState.from_received(msg)
            elif msg_type == 'open' and prev_state:
                state = prev_state.open(msg)
            elif msg_type == 'match' and prev_state:
                state = prev_state.match(msg)
            elif msg_type == 'change' and prev_state:
                state = prev_state.change(msg)
            elif msg_type == 'done' and prev_state:
                state = prev_state.done(msg)
            else:
                state = prev_state
            if state:
                self._orders[order_id] = state
            self._timestamp = max(self._timestamp, timestamp)

    def get_order_id(self, msg: dict) -> str:
        if 'order_id' in msg:
            order_id = msg['order_id']
        else:
            maker, taker = msg['maker_order_id'], msg['taker_order_id']
            order_id = maker if maker in self._orders else taker
        return order_id


class AsyncCoinbaseTracker(OrderTracker):
    def __init__(self, products: t.List[str], api_passphrase: str,
                 api_secret: str,
                 api_key: str, ignore_untracked: bool = True):
        self.ignore_untracked = ignore_untracked
        self._client = OrderTrackerClient(products=products,
                                          api_key=api_key,
                                          api_passphrase=api_passphrase,
                                          api_secret=api_secret)
        self._client.start()
        self.watchlist = set()

    def remember(self, order_id: str) -> None:
        self.watchlist.add(order_id)

    def barrier_snapshot(self) -> t.Tuple[datetime, dict]:
        if self._client.stop:
            raise ValueError()
        timestamp, snapshot = self._client.snapshot()
        if self.ignore_untracked:
            for order_id in list(snapshot.keys()):
                if order_id not in self.watchlist:
                    self._client.forget(order_id)
                    snapshot.pop(order_id)
        return timestamp, snapshot

    def snapshot(self) -> dict:
        _, snapshot = self.barrier_snapshot()
        logger.debug(f"Snapshot: {snapshot}")
        return snapshot

    def forget(self, order_id: str) -> None:
        if order_id in self.watchlist:
            self.watchlist.remove(order_id)
        self._client.forget(order_id)

    def stop(self) -> None:
        self._client.stop = True


def main():
    from trading.settings import coinbase as coinbase_settings

    products = [product['id'] for product in
                cbpro.PublicClient().get_products()
                if product['quote_currency'] == 'USD']
    tracker = AsyncCoinbaseTracker(products=products,
                                   api_key=coinbase_settings.API_KEY,
                                   api_secret=coinbase_settings.SECRET,
                                   api_passphrase=coinbase_settings.PASSPHRASE,
                                   ignore_untracked=True)
    while True:
        timestamp, snapshot = tracker.barrier_snapshot()
        print(sum(
            map(lambda o: Decimal(o['executed_value']), snapshot.values())))
        print(timestamp)
        time.sleep(5)


__all__ = ['AsyncCoinbaseTracker']

if __name__ == '__main__':
    main()
