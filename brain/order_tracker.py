from abc import ABC, abstractmethod
from datetime import datetime
import typing as t

import dateutil.parser

from cbpro import AuthenticatedClient


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
        timestamp = dateutil.parser.parse(self.client.get_time()['iso'])
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
                    self.forget(watch)
            else:
                break
        return snapshot

    def forget(self, order_id: str) -> None:
        self.watchlist.remove(order_id)
