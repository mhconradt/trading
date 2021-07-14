import typing as t
from abc import ABC, abstractmethod
from datetime import datetime

from cbpro import AuthenticatedClient

from helper.coinbase import get_server_time


class OrderTracker(ABC):
    @abstractmethod
    def remember(self, order_id: str) -> None:
        ...

    @abstractmethod
    def barrier_snapshot(self) -> t.Tuple[datetime, dict]:
        ...

    @abstractmethod
    def snapshot(self) -> dict:
        ...

    @abstractmethod
    def forget(self, order_id: str) -> None:
        ...


class SyncCoinbaseOrderTracker(OrderTracker):
    def __init__(self, client: AuthenticatedClient,
                 watchlist: t.Optional[t.List[str]] = None):
        self.client = client
        self.watchlist = [] if watchlist is None else watchlist

    @property
    def active_order_count(self) -> int:
        return len(self.watchlist)

    def remember(self, order_id: str) -> None:
        self.watchlist.append(order_id)

    def barrier_snapshot(self) -> t.Tuple[datetime, dict]:
        timestamp = get_server_time()
        return timestamp, self.snapshot()

    def snapshot(self) -> dict:
        index = len(self.watchlist) - 1
        watched_items = set(self.watchlist)
        snapshot = {}
        if not self.watchlist:
            return {}
        for order in self.client.get_orders(status='all'):
            for i in range(index, -1, -1):
                watch = self.watchlist[i]
                order_id = order['id']
                if watch == order_id:
                    index -= 1
                    snapshot[order_id] = order
                    break
                elif order_id not in watched_items and i:
                    # ignore this order
                    break
                else:
                    # watched item was canceled
                    index -= 1
                    self.forget(watch)
            else:
                break
        return snapshot

    def forget(self, order_id: str) -> None:
        if order_id in self.watchlist:
            self.watchlist.remove(order_id)
