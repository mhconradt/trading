from datetime import timedelta

import numpy as np
import pandas as pd
from influxdb_client import InfluxDBClient

from indicators.candles import CandleSticks


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
