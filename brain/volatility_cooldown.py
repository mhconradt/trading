import typing as t
from datetime import datetime, timedelta


class VolatilityCoolDown:
    def __init__(self, buy_period: timedelta = timedelta(0),
                 sell_period: timedelta = timedelta(0)):
        self.buy_period = buy_period
        self.sell_period = sell_period
        self.last_sold: t.Dict[str, datetime] = dict()
        self.last_bought: t.Dict[str, datetime] = dict()
        self.tick: t.Optional[datetime] = None

    def set_tick(self, tick: datetime) -> None:
        self.tick = tick

    def cooling_down(self, market: str) -> bool:
        if market not in self.last_bought:
            return False
        since_bought = (self.tick - self.last_bought[market])
        if since_bought < self.buy_period:
            return True
        if market not in self.last_sold:
            return False
        since_sold = (self.tick - self.last_sold[market])
        return since_sold < self.sell_period

    def sold(self, market: str) -> None:
        self.last_sold[market] = self.tick

    def bought(self, market: str) -> None:
        self.last_bought[market] = self.tick
