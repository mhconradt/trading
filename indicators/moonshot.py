from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient

from exceptions import StaleDataException
from .candles import CandleSticks
from .momentum import IncrementalMomentum as Momentum
from .ticker import Ticker


class MoonShot:
    def __init__(self, client: InfluxDBClient, exchange: str,
                 max_lag: timedelta):
        self.momentum_5m = Momentum(client, exchange,
                                    frequency=timedelta(minutes=5),
                                    start=timedelta(minutes=-15) - max_lag,
                                    stop=timedelta(0))
        self.momentum_15m = Momentum(client, exchange,
                                     frequency=timedelta(minutes=15),
                                     start=timedelta(minutes=-45) - max_lag,
                                     stop=timedelta(0))

    def compute(self) -> pd.Series:
        mom_5 = self.momentum_5m.compute()
        mom_15 = self.momentum_15m.compute()
        if not (len(mom_5) >= 2 and len(mom_15) >= 2):
            raise StaleDataException(f"Insufficient momentum for moonshots.")
        this_mom5 = mom_5.iloc[-1]
        this_mom15 = mom_15.iloc[-1]
        last_mom5 = mom_5.iloc[-2]
        last_mom15 = mom_15.iloc[-2]
        mom5_positive = (this_mom5 > 0.) & (last_mom5 > 0.)
        mom15_positive = (this_mom15 > 0.) & (last_mom15 > 0.)
        mom_positive = mom5_positive & mom15_positive
        mom_increasing = (this_mom5 > last_mom5) & (this_mom15 > last_mom15)
        buy_mask = mom_positive & mom_increasing
        mom5_diff = this_mom5 - last_mom5
        mom15_diff = this_mom15 - last_mom15
        mom5_estimate = (this_mom5 + last_mom5) / 2 + mom5_diff
        mom15_estimate = (this_mom15 + last_mom15) / 2 + mom15_diff
        mom_estimate = (mom5_estimate + mom15_estimate) / 2
        scores = buy_mask * mom_estimate
        return scores


class PessimisticMoonShot(MoonShot):
    def __init__(self, client: InfluxDBClient, exchange: str,
                 max_lag: timedelta):
        super().__init__(client, exchange, max_lag)
        self.long_mom = Momentum(client, exchange,
                                 frequency=timedelta(hours=1),
                                 start=timedelta(hours=-1),
                                 span=6)
        self.ticker = Ticker(client, exchange, start=timedelta(minutes=-1),
                             stop=timedelta(0))

    def compute(self) -> pd.Series:
        long_mom = self.long_mom.compute().iloc[-1]
        naive_scores = super(PessimisticMoonShot, self).compute()
        up = long_mom > 0.
        return naive_scores.where(up, 0.)
