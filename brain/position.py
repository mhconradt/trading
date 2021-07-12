import typing as t
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal


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
            previous_state = str(self.previous_state)
            return f"{previous_state} -> ({self.state_change}) -> {intrinsic}"
        else:
            return repr(self)


@dataclass(repr=False)
class RootState(PositionState):
    number: int
    market: str

    state_slug: str = 'root'
    state_change: t.Optional[str] = None
    previous_state: t.Optional[PositionState] = None

    def __repr__(self) -> str:
        return f"#{self.number}"


@dataclass(repr=False)
class Download(PositionState):
    number: int
    market: str

    state_slug: str = 'downloaded'

    state_change: t.Optional[str] = None
    previous_state: t.Optional[PositionState] = None

    def __repr__(self) -> str:
        return f"download #{self.number}"


@dataclass
class DesiredLimitBuy(PositionState):
    """
    We want to buy at most .size of base currency in .market for at most .price
    """

    price: Decimal
    size: Decimal

    market: str
    state_slug: str = 'desired_limit_buy'

    state_change: t.Optional[str] = field(default=None, repr=False)
    previous_state: t.Optional[PositionState] = field(default=None, repr=False)


@dataclass
class DesiredMarketBuy(PositionState):
    funds: Decimal
    market: str

    state_slug: str = 'desired_market_buy'

    state_change: t.Optional[str] = field(default=None, repr=False)
    previous_state: t.Optional[PositionState] = field(default=None, repr=False)


@dataclass
class PendingMarketBuy(PositionState):
    """
    We ordered .size of base currency in .market for .price * .size in quote.
    """
    funds: Decimal

    order_id: str
    created_at: datetime
    market: str
    state_slug: str = 'pending_market_buy'

    state_change: t.Optional[str] = field(default=None, repr=False)
    previous_state: t.Optional[PositionState] = field(default=None, repr=False)


@dataclass
class PendingLimitBuy(PositionState):
    """
    We ordered .size of base currency in .market for .price * .size in quote.
    """

    price: Decimal
    size: Decimal

    order_id: str
    created_at: datetime
    market: str
    state_slug: str = 'pending_limit_buy'

    state_change: t.Optional[str] = field(default=None, repr=False)
    previous_state: t.Optional[PositionState] = field(default=None, repr=False)


@dataclass
class PendingCancelBuy(PositionState):
    price: Decimal
    size: Decimal

    order_id: str
    created_at: datetime
    market: str
    state_slug: str = 'pending_cancel_buy'

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

    start: datetime
    market: str
    state_slug: str = 'active'

    state_change: t.Optional[str] = field(default=None, repr=False)
    previous_state: t.Optional[PositionState] = field(default=None, repr=False)

    def merge(self, other: "ActivePosition") -> "ActivePosition":
        if other.market != self.market:
            raise ValueError(f"{other.market} != {self.market}")
        fees = self.fees + other.fees
        size = self.size + other.size
        price = ((self.price * self.size) + (other.price * other.size)) / size
        start = min(self.start, other.start)
        state_change = 'merge'
        return ActivePosition(price=price, size=size, fees=fees, start=start,
                              market=self.market, state_change=state_change)

    def drawdown_clone(self, remainder: Decimal) -> "ActivePosition":
        fraction = (self.size - remainder) / self.size
        change = f"drawdown {fraction:.3f}"
        # note this short circuits history of draw-downs etc.
        return ActivePosition(self.price, remainder, self.fees, self.start,
                              self.market, self.state_slug, change,
                              previous_state=self.previous_state)


@dataclass
class DesiredMarketSell(PositionState):
    """
    Sell .size of .base at the market.
    """

    size: Decimal
    market: str
    state_slug: str = 'desired_market_sell'

    state_change: t.Optional[str] = field(default=None, repr=False)
    previous_state: t.Optional[PositionState] = field(default=None, repr=False)


@dataclass
class PendingMarketSell(PositionState):
    """
    Selling .size of .base at the market.
    """

    size: Decimal

    order_id: str
    created_at: datetime
    market: str
    state_slug: str = 'pending_market_sell'

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
    state_slug: str = 'desired_limit_sell'

    state_change: t.Optional[str] = field(default=None, repr=False)
    previous_state: t.Optional[PositionState] = field(default=None, repr=False)


@dataclass
class PendingLimitSell(PositionState):
    """
    We ordered .price * .size of .quote for .size in .base.
    """

    price: Decimal
    size: Decimal

    order_id: str
    created_at: datetime
    market: str
    state_slug: str = 'pending_limit_sell'

    state_change: t.Optional[str] = field(default=None, repr=False)
    previous_state: t.Optional[PositionState] = field(default=None, repr=False)


@dataclass
class PendingCancelLimitSell(PositionState):
    price: Decimal
    size: Decimal

    order_id: str
    created_at: datetime
    market: str
    state_slug: str = 'pending_cancel_limit_sell'

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
    state_slug: str = 'sold'

    state_change: t.Optional[str] = field(default=None, repr=False)
    previous_state: t.Optional[PositionState] = field(default=None, repr=False)


__all__ = ['DesiredMarketSell', 'DesiredLimitBuy', 'DesiredLimitSell',
           'RootState', 'PendingCancelBuy', 'PendingLimitSell',
           'PendingMarketSell', 'PendingLimitBuy', 'PendingCancelLimitSell',
           'Sold', 'PositionState', 'ActivePosition', 'Download',
           'PendingMarketBuy', 'DesiredMarketBuy']

if __name__ == '__main__':
    root = Download(number=1, market='BTC-USD')
    desired_buy = DesiredLimitBuy(market='BTC-USD', price=Decimal('42000.'),
                                  size=Decimal('1.234'), previous_state=root,
                                  state_change='buy STRONGLY indicated')
    pending_buy = PendingLimitBuy(market=desired_buy.market,
                                  price=desired_buy.price,
                                  size=desired_buy.size, order_id='order',
                                  created_at=datetime.now(),
                                  previous_state=desired_buy,
                                  state_change='order created')
    print(f"{pending_buy}")
