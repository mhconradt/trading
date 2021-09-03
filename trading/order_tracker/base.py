import typing as t
from abc import ABC, abstractmethod
from datetime import datetime


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

    @abstractmethod
    def stop(self) -> None:
        pass
