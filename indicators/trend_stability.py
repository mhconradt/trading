import numpy as np
import pandas as pd


def interval_intersection(x_lower: pd.Series, x_upper: pd.Series,
                          y_lower: pd.Series, y_upper: pd.Series) -> pd.Series:
    term0 = x_upper - x_lower
    x = x_upper - y_upper
    x1 = y_lower - x_lower
    term1 = np.maximum(x, 0.) + np.maximum(x1, 0.)
    x2 = y_lower - x_upper
    x3 = x_lower - y_upper
    term2 = np.maximum(x2, 0.) + np.maximum(x3, 0.)
    return term0 - term1 + term2


def compute_stability_scores(candles: pd.DataFrame) -> pd.Series:
    prd_open = candles.open.unstack('market').bfill().iloc[0]
    prd_close = candles.close.unstack('market').ffill().iloc[-1]
    prd_up = prd_close > prd_open
    prd_upper = prd_close.where(prd_up, prd_open)
    prd_lower = prd_open.where(prd_up, prd_close)
    cdl_open = candles.open
    cdl_close = candles.close
    cdl_up = cdl_close > cdl_open
    cdl_upper = cdl_close.where(cdl_up, cdl_open)
    cdl_lower = cdl_open.where(cdl_up, cdl_close)
    # the intersection of the candle "trend" and period "trend"
    cdl_reflected_trend = interval_intersection(cdl_lower, cdl_upper,
                                                prd_lower, prd_upper)
    ranges = candles.high - candles.low
    # the fraction of the intersection within the candle range
    # the idea here is to reflect both short and long term trend stability
    reflected_price_range = cdl_reflected_trend / ranges
    score = reflected_price_range.unstack('market').fillna(0.).mean()
    return score


class TrendStability:
    def __init__(self, periods: int):
        self.periods = periods

    @property
    def periods_required(self) -> int:
        return self.periods

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        candles = candles.unstack('market').tail(self.periods).stack('market')
        score = compute_stability_scores(candles)
        return score


def main():
    import time
    from datetime import timedelta

    import matplotlib.pyplot as plt
    from influxdb_client import InfluxDBClient

    import settings.influx_db as influx_db_settings
    from indicators.sliding_candles import CandleSticks

    influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                            influx_db_settings.INFLUX_TOKEN,
                            org_id=influx_db_settings.INFLUX_ORG_ID,
                            org=influx_db_settings.INFLUX_ORG)
    indicator = TrendStability(periods=5)
    candles = CandleSticks(influx, 'coinbasepro', 5, timedelta(minutes=1),
                           'level1', 'USD')
    while True:
        _start = time.time()
        results = indicator.compute(candles.compute()).sort_values()
        print(results.describe())
        results.plot.hist()
        plt.show()
        print(f"Took {time.time() - _start:.2f}s")


if __name__ == '__main__':
    main()
