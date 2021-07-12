import pandas as pd


class Ticker:
    def __init__(self):
        pass

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        closes = candles['close'].unstack('market')
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
    ticker = Ticker()
    candles = CandleSticks(_influx, 'coinbasepro', 1, timedelta(minutes=1))
    while True:
        _start = time.time()
        values = ticker.compute(candles.compute())
        print(values[values.index.str.endswith('-USD')])
        print(f"Took {time.time() - _start:.2f}s")


if __name__ == '__main__':
    main()
