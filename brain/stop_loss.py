from dataclasses import dataclass
from decimal import Decimal


@dataclass
class SimpleStopLoss:
    stop_loss: Decimal
    take_profit: Decimal

    def trigger_stop_loss(self, current_price: Decimal,
                          buy_price: Decimal) -> bool:
        return current_price / buy_price < self.stop_loss

    def trigger_take_profit(self, current_price: Decimal,
                            buy_price: Decimal) -> bool:
        return current_price / buy_price > self.take_profit
