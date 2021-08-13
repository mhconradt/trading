import numpy as np
import pandas as pd

from indicators.momentum import Momentum


class TrendVariance:
    def __init__(self, periods: int):
        self.periods = periods
        self.momentum = Momentum(self.periods)

    @property
    def periods_required(self) -> int:
        return max(self.periods, self.momentum.periods_required)

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        rates = self.momentum.compute(candles)
        rates_variance = rates.std()
        mean_roc = ((1. + rates).product() ** (1 / self.periods) - 1.).abs()
        score = np.log(mean_roc / rates_variance)
        return score


class AltTrendVariance:
    def __init__(self, periods: int):
        self.periods = periods
        self.momentum = Momentum(self.periods)

    @property
    def periods_required(self) -> int:
        return max(self.periods, self.momentum.periods_required)

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        rates = self.momentum.compute(candles)
        rates_variance = rates.std()
        mean_roc = ((1. + rates).product() - 1.).abs()
        score = np.log(mean_roc / rates_variance)
        return score


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
    indicator = TrendVariance(periods=5)
    alt_indicator = AltTrendVariance(periods=5)
    candles = CandleSticks(influx, 'coinbasepro', 5, timedelta(minutes=1),
                           'level1', 'USD')
    while True:
        _start = time.time()
        candle_data = candles.compute()
        results = indicator.compute(candle_data).sort_values()
        alt_results = alt_indicator.compute(candle_data)
        print(pd.DataFrame({'each': results, 'all': alt_results,
                            'delta': alt_results}).describe())
        print(f"Took {time.time() - _start:.2f}s")


if __name__ == '__main__':
    main()
