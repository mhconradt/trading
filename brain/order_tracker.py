from abc import ABC, abstractmethod
from collections import deque
from decimal import Decimal
from threading import Lock
import typing as t

from cbpro import AuthenticatedClient, WebsocketClient


class OrderTracker(ABC):
    @abstractmethod
    def track(self, order_id: str) -> None:
        ...

    @abstractmethod
    def barrier_snapshot(self) -> t.Tuple[str, dict]:
        ...

    @abstractmethod
    def snapshot(self) -> dict:
        ...

    @abstractmethod
    def un_track(self, order_id: str) -> None:
        ...


class SyncCoinbaseOrderTracker(OrderTracker):
    def barrier_snapshot(self) -> t.Tuple[str, dict]:
        timestamp = self.client.get_time()['iso']
        return timestamp, self.snapshot()

    def __init__(self, client: AuthenticatedClient,
                 watchlist: t.Optional[t.List[str]] = None):
        self.client = client
        self.watchlist = [] if watchlist is None else watchlist

    def track(self, order_id: str) -> None:
        self.watchlist.append(order_id)

    def snapshot(self) -> dict:
        index = len(self.watchlist) - 1
        snapshot = {}
        for order in self.client.get_orders(status='all'):
            for i in range(index, 0, -1):
                watch = self.watchlist[i]
                order_id = order['id']
                index -= 1
                if watch == order_id:
                    snapshot[order_id] = order
                    break
                else:
                    self.un_track(watch)
        return snapshot

    def un_track(self, order_id: str) -> None:
        self.watchlist.remove(order_id)
