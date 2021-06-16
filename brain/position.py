from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
import typing as t

from brain.stop_loss import BasicStopLoss

from abc import ABC, abstractmethod


class PositionState(ABC):
    @property
    @abstractmethod
    def previous_state(self) -> t.Optional["PositionState"]:
        pass

    @property
    @abstractmethod
    def state_change(self) -> t.Optional[str]:
        pass

    def __str__(self) -> str:
        if self.previous_state:
            intrinsic = repr(self)
            return f"{str(self.previous_state)} -> ({self.state_change}) -> {intrinsic}"
        else:
            return repr(self)


@dataclass(repr=False)
class RootState(PositionState):
    number: int

    state_change: t.Optional[str] = None
    previous_state: t.Optional[PositionState] = None

    def __repr__(self) -> str:
        return f"#{self.number}"


@dataclass
class DesiredLimitBuy(PositionState):
    """
    We want to buy at most .size of base currency in .market for at most .price
    """
    price: Decimal
    size: Decimal
    market: str

    state_change: t.Optional[str] = field(default=None, repr=False)
    previous_state: t.Optional[PositionState] = field(default=None, repr=False)


@dataclass
class PendingLimitBuy(PositionState):
    """
    We ordered .size of base currency in .market for .price * .size in quote.
    """
    price: Decimal
    size: Decimal
    market: str

    order_id: str
    created_at: datetime

    state_change: t.Optional[str] = field(default=None, repr=False)
    previous_state: t.Optional[PositionState] = field(default=None, repr=False)


@dataclass
class PendingCancelBuy(PositionState):
    price: Decimal
    size: Decimal
    market: str

    order_id: str
    created_at: datetime

    state_change: t.Optional[str] = field(default=None, repr=False)
    previous_state: t.Optional[PositionState] = field(default=None, repr=False)


@dataclass
class ActivePosition(PositionState):
    """
    We own .size of base currency in .base.
    We paid .price in .quote and .fees in .quote in fees.
    """
    price: Decimal
    size: Decimal
    fees: Decimal
    market: str

    start: datetime

    state_change: t.Optional[str] = field(default=None, repr=False)
    previous_state: t.Optional[PositionState] = field(default=None, repr=False)

    def __post_init__(self):
        self.stop_loss = BasicStopLoss(price=self.price, size=self.size)

    def stop_loss(self, price: Decimal) -> bool:
        return self.stop_loss.trigger_stop_loss(price)

    def take_profit(self, price: Decimal) -> bool:
        return self.stop_loss.trigger_take_profit(price)


@dataclass
class DesiredMarketSell(PositionState):
    """
    Sell .size of .base at the market.
    """
    size: Decimal
    market: str

    state_change: t.Optional[str] = field(default=None, repr=False)
    previous_state: t.Optional[PositionState] = field(default=None, repr=False)


@dataclass
class PendingMarketSell(PositionState):
    """
    Selling .size of .base at the market.
    """
    size: Decimal
    market: str

    order_id: str
    created_at: datetime

    state_change: t.Optional[str] = field(default=None, repr=False)
    previous_state: t.Optional[PositionState] = field(default=None, repr=False)


@dataclass
class DesiredLimitSell(PositionState):
    """
    Sell at most .size of .base for at least .price in .quote.
    """
    price: Decimal
    size: Decimal
    market: str

    state_change: t.Optional[str] = field(default=None, repr=False)
    previous_state: t.Optional[PositionState] = field(default=None, repr=False)


@dataclass
class PendingLimitSell(PositionState):
    """
    We ordered .price * .size of .quote for .size in .base.
    """
    price: Decimal
    size: Decimal
    market: str

    order_id: str
    created_at: datetime

    state_change: t.Optional[str] = field(default=None, repr=False)
    previous_state: t.Optional[PositionState] = field(default=None, repr=False)


@dataclass
class PendingCancelLimitSell(PositionState):
    size: Decimal
    market: str

    order_id: str
    created_at: datetime

    state_change: t.Optional[str] = field(default=None, repr=False)
    previous_state: t.Optional[PositionState] = field(default=None, repr=False)


@dataclass
class Sold(PositionState):
    """
    We sold .size of quote currency for .price * .size in base currency.
    """
    price: Decimal
    size: Decimal
    fees: Decimal
    market: str

    state_change: t.Optional[str] = field(default=None, repr=False)
    previous_state: t.Optional[PositionState] = field(default=None, repr=False)


if __name__ == '__main__':
    root = RootState(number=1)
    desired_buy = DesiredLimitBuy(market='BTC-USD', price=Decimal('42000.'),
                                  size=Decimal('1.234'), previous_state=root,
                                  state_change='buy STRONGLY indicated')
    pending_buy = PendingLimitBuy(market=desired_buy.market,
                                  price=desired_buy.price,
                                  size=desired_buy.size, order_id='abcdef',
                                  created_at=datetime.now(),
                                  previous_state=desired_buy,
                                  state_change='order created')
    print(f"{pending_buy}")
