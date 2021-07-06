from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient

from indicators.candles import CandleSticks


# TODO: Refactor all indicators to internalize lag logic at lowest level


def min0(x: pd.Series) -> pd.Series:
    return x.where(x >= 0., 0.)


def interval_intersection(x_lower: pd.Series, x_upper: pd.Series,
                          y_lower: pd.Series, y_upper: pd.Series) -> pd.Series:
    term0 = x_upper - x_lower
    term1 = min0(x_upper - y_upper) + min0(y_lower - x_lower)
    term2 = min0(y_lower - x_upper) + min0(x_lower - y_upper)
    return term0 - term1 + term2


# note: time period???


def compute_stability_scores(candles):
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
    score = reflected_price_range.unstack('market').mean()
    return score


class TrendStability:
    def __init__(self, client: InfluxDBClient, exchange: str, periods: int,
                 frequency: timedelta):
        self.periods = periods
        self.candles = CandleSticks(client, exchange, periods=self.periods,
                                    frequency=frequency)

    def compute(self) -> pd.Series:
        candles = self.candles.compute()
        score = compute_stability_scores(candles)
        return score


if __name__ == '__main__':
    import time
    import matplotlib.pyplot as plt
    import settings.influx_db as influx_db_settings

    influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                            influx_db_settings.INFLUX_TOKEN,
                            org_id=influx_db_settings.INFLUX_ORG_ID,
                            org=influx_db_settings.INFLUX_ORG)

    indicator = TrendStability(influx, 'coinbasepro', 5, timedelta(minutes=1))
    while True:
        results = indicator.compute().sort_values()
        print(results.describe())
        print(f"LTC: {results.loc['LTC-USD']}")
        print(f"SUSHI: {results.loc['SUSHI-USD']}")  # 16:03 to 16:07
        results.plot.hist()
        plt.show()
        time.sleep(60)
