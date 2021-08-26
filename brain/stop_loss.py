from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal


class StopLoss(ABC):
    @abstractmethod
    def trigger(self, current_price: Decimal, buy_price: Decimal) -> bool:
        pass


@dataclass
class SimpleStopLoss(StopLoss):
    stop_loss: Decimal

    def trigger(self, current_price: Decimal, buy_price: Decimal) -> bool:
        return current_price / buy_price < self.stop_loss
