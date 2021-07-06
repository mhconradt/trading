from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient

from .candles import CandleSticks


class TrailingVolume:
    def __init__(self, client: InfluxDBClient, exchange: str, periods: int,
                 frequency: timedelta):
        self.candles = CandleSticks(client, exchange, periods=periods,
                                    frequency=frequency)

    def compute(self) -> pd.Series:
        candles = self.candles.compute()
        return candles.volume.groupby(level='market').sum()
