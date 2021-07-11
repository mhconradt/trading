from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient

from indicators.candles import CandleSticks


class TrailingVolume:
    def __init__(self, client: InfluxDBClient, exchange: str, periods: int,
                 frequency: timedelta):
        self.candles = CandleSticks(client, exchange, periods=periods,
                                    frequency=frequency)

    def compute(self) -> pd.Series:
        candles = self.candles.compute()
        return candles.volume.groupby(level='market').sum()


class TrailingQuoteVolume:
    def __init__(self, client: InfluxDBClient, exchange: str, periods: int,
                 frequency: timedelta):
        self.candles = CandleSticks(client, exchange, periods=periods,
                                    frequency=frequency)

    def compute(self) -> pd.Series:
        candles = self.candles.compute()
        return candles.quote_volume.groupby(level='market').sum()


def main(influx: InfluxDBClient):
    import time
    candles = TrailingQuoteVolume(influx, 'coinbasepro', 5,
                                  timedelta(minutes=1))
    total = 0.
    measurements = 7
    for i in range(measurements):
        start = time.time()
        values = candles.compute()
        print(values)
        total += time.time() - start
    print(total / measurements)


if __name__ == '__main__':
    from settings import influx_db as influx_db_settings

    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    main(influx_client)
