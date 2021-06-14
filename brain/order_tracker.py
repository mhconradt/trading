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
    def untrack(self, order_id: str) -> None:
        ...


class SyncCoinbaseOrderTracker(OrderTracker):
    def __init__(self, client: AuthenticatedClient,
                 watchlist: t.Optional[t.List[str]] = None):
        self.client = client
        self.watchlist = [] if watchlist is None else watchlist

    @property
    def active_order_count(self) -> int:
        return len(self.watchlist)

    def track(self, order_id: str) -> None:
        self.watchlist.append(order_id)

    def barrier_snapshot(self) -> t.Tuple[str, dict]:
        timestamp = self.client.get_time()['iso']
        return timestamp, self.snapshot()

    def snapshot(self) -> dict:
        index = len(self.watchlist) - 1
        watched_items = set(self.watchlist)
        snapshot = {}
        for order in self.client.get_orders(status='all'):
            print(order['id'])
            for i in range(index, -1, -1):
                watch = self.watchlist[i]
                order_id = order['id']
                print(f"{order_id} -> {watch}")
                if watch == order_id:
                    index -= 1
                    snapshot[order_id] = order
                    break
                elif order_id not in watched_items:
                    # ignore this order
                    break
                else:
                    # watched item was canceled
                    index -= 1
                    self.un_track(watch)
            else:
                break
        return snapshot

    def untrack(self, order_id: str) -> None:
        self.watchlist.remove(order_id)
