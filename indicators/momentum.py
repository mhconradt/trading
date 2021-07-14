import pandas as pd


class Momentum:
    def __init__(self, periods: int, span: int = 1):
        self.periods = periods
        self.span = span

    @property
    def periods_required(self) -> int:
        return self.periods + self.span

    def compute(self, candles: pd.DataFrame) -> pd.DataFrame:
        closes = candles['close'].unstack('market')
        momentum = closes.pct_change(self.span)
        return momentum.tail(self.periods)


class VWAMomentum:
    def __init__(self, periods: int, span: int = 1):
        self.periods = periods
        self.span = span

    @property
    def periods_required(self) -> int:
        return self.periods + self.span

    def compute(self, candles: pd.DataFrame) -> pd.DataFrame:
        prices = (candles.quote_volume / candles.volume).unstack('market')
        momentum = prices.pct_change(self.span)
        return momentum.tail(self.periods)


def main():
    from datetime import timedelta

    from influxdb_client import InfluxDBClient

    from settings import influx_db as influx_db_settings
    from indicators.sliding_candles import CandleSticks

    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)

    mom = Momentum(periods=5)
    candles_src = CandleSticks(influx_client, 'coinbasepro',
                               frequency=timedelta(minutes=1), periods=6)
    while True:
        print(mom.compute(candles_src.compute()))


if __name__ == '__main__':
    main()
