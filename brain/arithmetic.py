import functools as ft
from dataclasses import dataclass
from decimal import Decimal


@dataclass
class Buy:
    market: str
    size: Decimal
    cost: Decimal


@dataclass
class Sell:
    market: str
    size: Decimal
    cost: Decimal


@dataclass
class Position:
    market: str
    size: Decimal
    cost: Decimal

    @classmethod
    def zero(cls, market: str) -> "Position":
        return Position(market=market, size=Decimal('0'), cost=Decimal('0'))

    @property
    def price(self) -> Decimal:
        return self.cost / self.size

    @ft.singledispatchmethod
    def __add__(self, other: object) -> "Position":
        if not isinstance(other, Position):
            raise TypeError()
        if other.market != self.market:
            raise ValueError("Markets must be equal")
        return Position(market=self.market, cost=self.cost + other.cost,
                        size=self.size + other.size)

    @__add__.register
    def _(self, other: Buy):
        if other.market != self.market:
            raise ValueError("Markets must be equal")
        return Position(self.market, cost=self.cost + other.cost,
                        size=self.size + other.size)

    @__add__.register
    def _(self, other: Sell):
        if other.market != self.market:
            raise ValueError("Markets must be equal")
        return Position(self.market, cost=self.cost - other.cost,
                        size=self.size - other.size)


if __name__ == '__main__':
    position = Position.zero('BTC-USD')
    position += Buy('BTC-USD', size=Decimal('1.0'), cost=Decimal('33000.0'))
    print(position)
    position += Sell('BTC-USD', size=Decimal('1.0'), cost=Decimal('60000.0'))
    print(position)
