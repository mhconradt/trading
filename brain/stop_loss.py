from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
import time


@dataclass
class StopLoss:
    price: Decimal
    size: Decimal
    fees: Decimal

    start: datetime

    stop_loss: Decimal = Decimal('0.965')
    take_profit: Decimal = Decimal('1.01')

    time: float = field(default_factory=time.monotonic)

    def __post_init__(self):
        self.last_check = self.start

    def trigger(self, price: Decimal, fee: Decimal, at: datetime) -> bool:
        if at - self.last_check > timedelta(seconds=15):
            ...
