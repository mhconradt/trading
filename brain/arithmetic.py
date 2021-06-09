import functools as ft
from dataclasses import dataclass
from decimal import Decimal


@dataclass
class Buy:
    size: Decimal
    cost: Decimal


@dataclass
class Sell:
    size: Decimal
    cost: Decimal


@dataclass
class Position:
    size: Decimal = Decimal('0')
    cost: Decimal = Decimal('0')

    @property
    def price(self) -> Decimal:
        return self.cost / self.size

    @ft.singledispatchmethod
    def __add__(self, other: object) -> "Position":
        if not isinstance(other, Position):
            raise TypeError()
        return Position(cost=self.cost + other.cost,
                        size=self.size + other.size)

    @__add__.register
    def _(self, other: Buy):
        return Position(cost=self.cost + other.cost,
                        size=self.size + other.size)

    @__add__.register
    def _(self, other: Sell):
        return Position(cost=self.cost - other.cost,
                        size=self.size - other.size)


if __name__ == '__main__':
    position = Position()
    position += Buy(size=Decimal('1.0'), cost=Decimal('33000.0'))
    print(position)
    position += Sell(size=Decimal('1.0'), cost=Decimal('60000.0'))
    print(position)
