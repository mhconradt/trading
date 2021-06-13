from abc import ABC, abstractmethod
from decimal import Decimal
import typing as t

from cbpro import WebsocketClient


class OrderTracker(ABC):
    @abstractmethod
    def track(self, order_id: str) -> None:
        ...

    @abstractmethod
    def done(self, order_id: str) -> bool:
        ...

    @abstractmethod
    def remaining_size(self, order_id: str) -> Decimal:
        ...

    @abstractmethod
    def un_track(self, order_id: str) -> None:
        ...


class CoinbaseOrderTracker(OrderTracker):
    def track(self, order_id: str) -> None:
        self.watched.add(order_id)

    def done(self, order_id: str) -> bool:
        return self.server_done[self.client_server_mapping[order_id]]

    def remaining_size(self, order_id: str) -> Decimal:
        return self.server_remaining_size[self.client_server_mapping[order_id]]

    def un_track(self, order_id: str) -> None:
        self.watched.remove(order_id)
        server_id = self.client_server_mapping[order_id]
        self.server_done.pop(server_id)
        self.server_remaining_size.pop(server_id)

    def __init__(self, products: t.List[str], api_key: str, api_secret: str,
                 api_passphrase: str):
        self.ws_client = WebsocketClient(products=products,
                                         channels=['user', 'heartbeat'],
                                         api_key=api_key,
                                         api_secret=api_secret,
                                         api_passphrase=api_passphrase,
                                         auth=True)
        self.watched = set()
        self.client_server_mapping = {}
        self.server_remaining_size = {}
        self.server_done = {}
