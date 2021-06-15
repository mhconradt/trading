from datetime import datetime, timedelta
import typing as t


class VolatilityCoolDown:
    def __init__(self, period: timedelta = timedelta(0)):
        self.period = period
        self.last_sold: t.Dict[str, datetime] = {}
        self.tick: t.Optional[datetime] = None

    def set_tick(self, tick: datetime) -> None:
        self.tick = tick

    def cooling_down(self, market: str) -> bool:
        if market not in self.last_sold:
            return True
        return self.tick - self.last_sold[market] > self.period

    def sold(self, market: str) -> None:
        self.last_sold[market] = self.tick
