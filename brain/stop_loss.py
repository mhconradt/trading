from dataclasses import dataclass
from decimal import Decimal


@dataclass
class BasicStopLoss:
    price: Decimal
    size: Decimal

    stop_loss: Decimal = Decimal('0.975')
    take_profit: Decimal = Decimal('1.0125')

    def trigger(self, price: Decimal) -> bool:
        return not self.stop_loss < price / self.price < self.take_profit
