from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import typing as t

from brain.stop_loss import BasicStopLoss


@dataclass
class DesiredLimitBuy:
    """
    We want to buy at most .size of base currency in .market for at most .price
    """
    price: Decimal
    size: Decimal
    market: str

    history: t.List[object]


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

    history: t.List[object]


@dataclass
class PendingCancelBuy:
    price: Decimal
    size: Decimal
    market: str

    order_id: str
    created_at: datetime

    history: t.List[object]


@dataclass
class ActiveStopLossPosition:
    price: Decimal
    size: Decimal
    fees: Decimal
    market: str

    stop_loss_order_id: str
    take_profit_order_id: str

    history: t.List[object]


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

    history: t.List[object]

    def __post_init__(self):
        self.stop_loss = BasicStopLoss(price=self.price, size=self.size)

    def sell(self, price: Decimal) -> bool:
        return self.stop_loss.trigger(price)


@dataclass
class DesiredMarketSell:
    """
    Sell .size of .base at the market.
    """
    size: Decimal
    market: str

    history: t.List[object]


@dataclass
class PendingMarketSell:
    """
    Selling .size of .base at the market.
    """
    size: Decimal
    market: str

    order_id: str
    created_at: datetime

    history: t.List[object]


@dataclass
class DesiredLimitSell:
    """
    Sell at most .size of .base for at least .price in .quote.
    """
    price: Decimal
    size: Decimal
    market: str

    history: t.List[object]


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

    history: t.List[object]


@dataclass
class PendingCancelLimitSell:
    size: Decimal
    market: str

    order_id: str
    created_at: datetime

    history: t.List[object]


@dataclass
class Sold:
    """
    We sold .size of quote currency for .price * .size in base currency.
    """
    price: Decimal
    size: Decimal
    fees: Decimal
    market: str

    history: t.List[object]
