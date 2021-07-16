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
        snapshot = {}
        for order_id in self.watchlist:
            order = self.client.get_order(order_id)
            if order.get('message') == 'NotFound':
                self.forget(order_id)
            else:
                snapshot[order_id] = order
        return snapshot

    def forget(self, order_id: str) -> None:
        if order_id in self.watchlist:
            self.watchlist.remove(order_id)


def main():
    from helper.coinbase import AuthenticatedClient
    from settings import coinbase as coinbase_settings

    client = AuthenticatedClient(passphrase=coinbase_settings.PASSPHRASE,
                                 key=coinbase_settings.API_KEY,
                                 b64secret=coinbase_settings.SECRET)
    o = client.get_order('3fc03072-924b-40f7-9cff-febc8546fefc')
    print(o)


if __name__ == '__main__':
    main()
