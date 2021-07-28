from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient

from indicators.candles import CandleSticks


class RollingVolumeWeightedPrice:
    def __init__(self, periods: int, k: int):
        self.periods = periods
        self.k = k

    @property
    def periods_required(self) -> int:
        return self.periods + self.k

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        v = candles.volume.unstack('market')
        qv = candles.quote_volume.unstack('market')
        rolling_v = v.rolling(self.k, min_periods=1).sum()
        rolling_qv = qv.rolling(self.k, min_periods=1).sum()
        prices = rolling_qv / rolling_v
        return prices.tail(self.periods)


class RollingPrice:
    def __init__(self, periods: int, k: int):
        self.periods = periods
        self.k = k

    @property
    def periods_required(self) -> int:
        return self.periods + self.k

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        closes = candles.close.unstack('market')
        prices = closes.ewm(span=self.k).mean()
        return prices.tail(self.periods)


class Price:
    def __init__(self, periods: int):
        self.periods = periods

    @property
    def periods_required(self) -> int:
        return self.periods

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        closes = candles.close.unstack('market')
        return closes.tail(self.periods)


def main(influx: InfluxDBClient):
    import time
    import matplotlib.pyplot as plt

    candles = CandleSticks(influx, 'coinbasepro', 86, timedelta(minutes=1),
                           offset=0)
    periods = 60
    short, long = 12, 26
    long_uw = RollingPrice(periods=periods, k=long)
    short_uw = RollingPrice(periods=periods, k=short)
    while True:
        start = time.time()
        values = candles.compute()
        mkt = 'ETH-USD'
        prices = values.close.unstack('market').tail(periods)[mkt]
        long_values = long_uw.compute(values)[mkt]
        short_values = short_uw.compute(values)[mkt]
        pd.DataFrame({'long': long_values, 'short': short_values,
                      'price': prices}).plot(title='Standard Moving Average')
        plt.show()
        print(f"Took {time.time() - start:.2f}s")
        time.sleep(60)


if __name__ == '__main__':
    from settings import influx_db as influx_db_settings

    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    main(influx_client)
