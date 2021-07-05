from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient

from indicators.candles import CandleSticks


# TODO: Refactor all indicators to internalize lag logic at lowest level


class Turbulence:
    def __init__(self, client: InfluxDBClient, exchange: str, periods: int,
                 frequency: timedelta):
        start = -(periods + 1) * frequency + timedelta(seconds=15)
        self.periods = periods
        self.candles = CandleSticks(client, exchange, frequency=frequency,
                                    start=start)

    def compute(self) -> pd.Series:
        candles = self.candles.compute().iloc[-self.periods:]
        ranges = (candles.high - candles.low)
        moves = (candles.open - candles.close)
        return (moves / ranges).unstack('market').mean()


if __name__ == '__main__':
    import time
    import matplotlib.pyplot as plt
    import settings.influx_db as influx_db_settings

    influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                            influx_db_settings.INFLUX_TOKEN,
                            org_id=influx_db_settings.INFLUX_ORG_ID,
                            org=influx_db_settings.INFLUX_ORG)

    indicator = Turbulence(influx, 'coinbasepro', 5, timedelta(minutes=1))
    while True:
        results = indicator.compute().sort_values()
        print(results.describe())
        print(results.head(10))
        print(results.tail(10))
        results.plot.hist()
        plt.show()
        time.sleep(60)
