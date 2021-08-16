import logging
import typing as t
from datetime import datetime

from cbpro import AuthenticatedClient

from helper.coinbase import get_server_time
from order_tracker.base import OrderTracker

logger = logging.getLogger(__name__)


class SyncCoinbaseTracker(OrderTracker):
    def __init__(self, client: AuthenticatedClient,
                 watchlist: t.Optional[t.List[str]] = None):
        self.client = client
        self.watchlist = [] if watchlist is None else watchlist

    @property
    def active_order_count(self) -> int:
        return len(self.watchlist)

    def remember(self, order_id: str) -> None:
        logger.debug(f"Tracking {order_id}")
        self.watchlist.append(order_id)

    def barrier_snapshot(self) -> t.Tuple[datetime, dict]:
        timestamp = get_server_time()
        return timestamp, self.snapshot()

    def snapshot(self) -> dict:
        snapshot = {}
        for order_id in self.watchlist.copy():
            order = self.client.get_order(order_id)
            if order.get('message') == 'NotFound':
                self.forget(order_id)
            else:
                snapshot[order_id] = order
        return snapshot

    def forget(self, order_id: str) -> None:
        if order_id in self.watchlist:
            logger.debug(f"Forgetting {order_id}")
            self.watchlist.remove(order_id)

    def stop(self) -> None:
        pass
