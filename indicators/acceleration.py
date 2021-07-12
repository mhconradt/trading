import numpy as np
import pandas as pd

from indicators.momentum import Momentum


class TrendAcceleration:
    """
    Detects reversal of a trend. i.e. positive turns negative.
    """

    def __init__(self, a: int = 1, b: int = 1, trend_sign: int = 1):
        self.momentum = Momentum(periods=a + b)
        self.a = a
        self.b = b
        self.trend_sign = trend_sign

    @property
    def periods_required(self) -> int:
        return self.momentum.periods_required

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        momentum = self.momentum.compute(candles)
        a, b = momentum.iloc[:self.a], momentum.iloc[self.a:self.a + self.b]
        a, b = a + 1, b + 1
        # geometric mean
        a, b = a.product() ** (1 / self.a), b.product() ** (1 / self.b)
        a, b = a - 1, b - 1
        if self.trend_sign:
            return b / a * np.sign(a) * self.trend_sign
        else:
            return b / a


def main():
    import time
    from datetime import timedelta

    from influxdb_client import InfluxDBClient

    import settings.influx_db as influx_db_settings
    from indicators.sliding_candles import CandleSticks
    influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                            influx_db_settings.INFLUX_TOKEN,
                            org_id=influx_db_settings.INFLUX_ORG_ID,
                            org=influx_db_settings.INFLUX_ORG)
    indicator = TrendAcceleration(a=3, b=2)  # fib(4), fib(3)
    candles = CandleSticks(influx, 'coinbasepro', 6, timedelta(minutes=1))
    while True:
        _start = time.time()
        print(indicator.compute(candles.compute()))
        print(f"Took {time.time() - _start:.2f}s")


if __name__ == '__main__':
    main()
