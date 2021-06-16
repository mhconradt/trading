from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient

from .momentum import Momentum


class MoonShot:
    def __init__(self, client: InfluxDBClient, exchange: str = 'coinbasepro'):
        self.momentum_5m = Momentum(client, exchange,
                                    frequency=timedelta(minutes=5),
                                    start=timedelta(minutes=-30),
                                    stop=timedelta(0))
        self.momentum_15m = Momentum(client, exchange,
                                     frequency=timedelta(minutes=15),
                                     start=timedelta(minutes=-60),
                                     stop=timedelta(0))

    def compute(self) -> pd.Series:
        mom_5 = self.momentum_5m.compute()
        mom_15 = self.momentum_15m.compute()
        this_mom5 = mom_5.iloc[-1]
        this_mom15 = mom_15.iloc[-1]
        last_mom5 = mom_5.iloc[-2]
        last_mom15 = mom_15.iloc[-2]
        mom5_increasing = (this_mom5 > 0.) & (last_mom5 > 0.)
        mom15_increasing = (this_mom15 > 0.) & (last_mom15 > 0.)
        increasing = mom5_increasing & mom15_increasing
        accelerating = (this_mom5 > last_mom5) & (this_mom15 > last_mom15)
        buy_mask = increasing & accelerating
        mom5_diff = this_mom5 - last_mom5
        mom15_diff = this_mom15 - last_mom15
        scores = buy_mask * (mom5_diff + mom15_diff)
        return scores
