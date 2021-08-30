import logging
import typing as t
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class CoolDown:
    def __init__(self, buy_period: timedelta = timedelta(0),
                 sell_period: timedelta = timedelta(0)):
        self.buy_period = buy_period
        self.sell_period = sell_period
        self.last_sold: t.Dict[str, datetime] = dict()
        self.last_bought: t.Dict[str, datetime] = dict()
        self.tick: t.Optional[datetime] = None

    def set_tick(self, tick: datetime) -> None:
        markets = {*self.last_sold, *self.last_bought}
        remaining = {market: self.cooling_down(market) for market in markets}
        remaining = {market: period
                     for market, period in remaining.items() if period}
        logger.debug(f"Cooling down: {remaining}")
        self.tick = tick

    def cooling_down(self, market: str) -> timedelta:
        remainder = timedelta(0)
        since_sold = self.tick - self.last_sold.get(market, self.tick)
        remainder = max(remainder, since_sold)
        since_bought = self.tick - self.last_bought.get(market, self.tick)
        remainder = max(remainder, since_bought)
        return remainder

    def sold(self, market: str) -> None:
        self.last_sold[market] = self.tick

    def bought(self, market: str) -> None:
        self.last_bought[market] = self.tick
