from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import typing as t

from brain.stop_loss import StopLoss


class History(t.Protocol):
    history: t.List["History"]


@dataclass
class DesiredLimitBuy:
    """
    We want to buy at most .size of base currency in .market for at most .price
    """
    price: Decimal
    size: Decimal
    market: str

    history: t.List[History]


@dataclass
class PendingLimitBuy:
    """
    We ordered .size of base currency in .market for .price * .size in quote.
    """
    price: Decimal
    size: Decimal
    market: str

    order_id: str
    created_at: datetime

    history: t.List[History]


@dataclass
class PendingCancelBuy:
    price: Decimal
    size: Decimal
    market: str

    order_id: str
    created_at: datetime

    history: t.List[History]


@dataclass
class ActivePosition:
    """
    We own .size of base currency in .base.
    We paid .price in .quote and .fees in .quote in fees.
    """
    price: Decimal
    size: Decimal
    fees: Decimal
    market: str

    start: datetime

    history: t.List[History]

    def __post_init__(self):
        self.stop_loss = StopLoss(price=self.price, size=self.size,
                                  fees=self.fees)

    def sell(self, price: Decimal, fee: Decimal) -> bool:
        return self.stop_loss.trigger(price, fee)


@dataclass
class DesiredLimitSell:
    """
    Sell at most .size of .base for at least .price in .quote.
    """
    price: Decimal
    size: Decimal
    market: str

    history: t.List[History]


@dataclass
class PendingLimitSell:
    """
    We ordered .price * .size of .quote for .size in .base.
    """
    price: Decimal
    size: Decimal
    market: str

    order_id: str
    created_at: datetime

    history: t.List[History]


@dataclass
class PendingCancelSell:
    price: Decimal
    size: Decimal
    market: str

    order_id: str
    created_at: datetime

    history: t.List[History]


@dataclass
class Sold:
    """
    We sold .size of quote currency for .price * .size in base currency.
    """
    price: Decimal
    size: Decimal
    fees: Decimal
    market: str

    history: t.List[History]
