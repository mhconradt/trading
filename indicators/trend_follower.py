from datetime import timedelta

import numpy as np
import pandas as pd
from influxdb_client import InfluxDBClient

from .sliding_momentum import SlidingMomentum


class TrendFollower:
    """
    Detects reversal of a trend. i.e. positive turns negative.
    """

    def __init__(self, client: InfluxDBClient,
                 frequency: timedelta = timedelta(minutes=1),
                 a: int = 1,
                 b: int = 1, trend_sign: int = 1):
        self.client = client
        self.sliding_momentum = SlidingMomentum(client, frequency=frequency,
                                                periods=a + b)
        self.a = a
        self.b = b
        self.trend_sign = trend_sign

    def compute(self) -> pd.Series:
        momentum = self.sliding_momentum.compute()
        a, b = momentum.iloc[:self.a], momentum.iloc[self.a:self.a + self.b]
        a, b = a + 1, b + 1
        # geometric mean
        a, b = a.product() ** (1 / self.a), b.product() ** (1 / self.b)
        a, b = a - 1, b - 1
        if self.trend_sign:
            return b / a * np.sign(a) * self.trend_sign
        else:
            return b / a


if __name__ == '__main__':
    import settings.influx_db as influx_db_settings

    influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                            influx_db_settings.INFLUX_TOKEN,
                            org_id=influx_db_settings.INFLUX_ORG_ID,
                            org=influx_db_settings.INFLUX_ORG)

    indicator = TrendFollower(influx, a=3, b=2)  # fib(4), fib(3)
    while True:
        print(indicator.compute())
