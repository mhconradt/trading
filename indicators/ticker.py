import pandas as pd


class Ticker:
    def __init__(self, periods: int):
        self.periods = periods

    @property
    def periods_required(self) -> int:
        return self.periods

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        closes = candles['close'].unstack('market')
        fill_limit = self.periods - 1
        if fill_limit:
            closes = closes.ffill(limit=fill_limit)
        return closes.iloc[-1]


def main():
    import time
    from datetime import timedelta

    from influxdb_client import InfluxDBClient

    from indicators.sliding_candles import CandleSticks
    from settings import influx_db as influx_db_settings
    _influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                             influx_db_settings.INFLUX_TOKEN,
                             org_id=influx_db_settings.INFLUX_ORG_ID,
                             org=influx_db_settings.INFLUX_ORG)
    ticker = Ticker(1)
    candles = CandleSticks(_influx, 'coinbasepro', 1, timedelta(minutes=1),
                           'level1', 'USD')
    while True:
        _start = time.time()
        values = ticker.compute(candles.compute())
        print(values[values.index.str.endswith('-USD')])
        print(f"Took {time.time() - _start:.2f}s")


if __name__ == '__main__':
    main()
