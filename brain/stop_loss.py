from dataclasses import dataclass
from decimal import Decimal


@dataclass
class BasicStopLoss:
    price: Decimal
    size: Decimal

    stop_loss: Decimal = Decimal('0.975')
    take_profit: Decimal = Decimal('1.0125')

    def trigger_stop_loss(self, price: Decimal) -> bool:
        return price / self.price < self.stop_loss

    def trigger_take_profit(self, price: Decimal) -> bool:
        return price / self.price > self.take_profit
