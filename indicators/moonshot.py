from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient

from exceptions import StaleDataException
from .momentum import IncrementalMomentum
from .protocols import RangeIndicator


class MoonShot:
    def __init__(self, client: InfluxDBClient, exchange: str,
                 max_lag: timedelta):
        short_start = timedelta(minutes=-15) - max_lag
        short_freq = timedelta(minutes=5)
        self.momentum_5m = IncrementalMomentum(client, exchange,
                                               frequency=short_freq,
                                               start=short_start,
                                               stop=timedelta(0))
        long_start = timedelta(minutes=-45) - max_lag
        long_freq = timedelta(minutes=15)
        self.momentum_15m = IncrementalMomentum(client, exchange,
                                                frequency=long_freq,
                                                start=long_start,
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
        mom5_estimate = last_mom5 + mom5_diff
        mom15_estimate = last_mom15 + mom15_diff
        mom_estimate = (mom5_estimate + mom15_estimate) / 2
        scores = buy_mask * mom_estimate
        return scores


class PessimisticMoonShot(MoonShot):
    def __init__(self, client: InfluxDBClient, exchange: str,
                 max_lag: timedelta, long_trend: RangeIndicator):
        super().__init__(client, exchange, max_lag)
        self.long_trend = long_trend

    def compute(self) -> pd.Series:
        long_trend = self.long_trend.compute().iloc[-1]
        naive_scores = super(PessimisticMoonShot, self).compute()
        trending_up = long_trend > 0.
        return naive_scores.where(trending_up, 0.)
