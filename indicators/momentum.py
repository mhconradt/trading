from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient

from indicators.candles import CandleSticks
from indicators.ticker import Ticker


class Momentum:
    def __init__(self, db: InfluxDBClient, exchange: str, periods: int,
                 frequency: timedelta):
        self.db = db
        self.frequency = frequency
        self.exchange = exchange
        self.periods = periods
        self.candles = CandleSticks(db, exchange, self.periods + 1, frequency)

    def compute(self) -> pd.DataFrame:
        closes = self.candles.compute()['close'].unstack('market')
        momentum = closes.pct_change()
        return momentum.tail(self.periods)


class IncrementalMomentum:
    def __init__(self, db: InfluxDBClient, exchange: str, periods: int,
                 frequency: timedelta, span: int = 1):
        self.ticker = Ticker(db, exchange)
        self.candles = CandleSticks(db, exchange,
                                    periods=self.periods + self.span,
                                    frequency=frequency)
        self.periods = periods
        self.span = span

    def compute(self) -> pd.DataFrame:
        close = self.candles.compute()['close'].unstack('market')
        ticker = self.ticker.compute()
        closes = close.append(ticker)
        return closes.pct_change(self.span).tail(self.periods)


def main(influx: InfluxDBClient):
    mom = IncrementalMomentum(influx, 'coinbasepro', periods=5,
                              frequency=timedelta(minutes=1))
    print(mom.compute())


if __name__ == '__main__':
    from settings import influx_db as influx_db_settings

    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    main(influx_client)
