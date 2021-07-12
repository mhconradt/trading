import pandas as pd
from influxdb_client import InfluxDBClient


class TrailingVolume:
    def __init__(self, periods: int):
        self.periods = periods

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        candles = candles.unstack('market').tail(self.periods).stack('market')
        return candles.volume.groupby(level='market').sum()


class TrailingQuoteVolume:
    def __init__(self, periods: int):
        self.periods = periods

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        candles = candles.unstack('market').tail(self.periods).stack('market')
        return candles.quote_volume.groupby(level='market').sum()


def main():
    import time
    from datetime import timedelta

    from settings import influx_db as influx_db_settings
    from indicators.sliding_candles import CandleSticks

    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    quote_volume = TrailingQuoteVolume(5)
    volume = TrailingVolume(5)
    total = 0.
    src = CandleSticks(influx_client, 'coinbasepro', 5,
                       timedelta(minutes=1))
    while True:
        start = time.time()
        candles = src.compute()
        values = quote_volume.compute(candles)
        print(values)
        values = volume.compute(candles)
        print(values)
        total += time.time() - start


if __name__ == '__main__':
    main()
