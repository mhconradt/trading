from datetime import timedelta

import numpy as np
import pandas as pd
from influxdb_client import InfluxDBClient

from indicators.sliding_momentum import SlidingMomentum
from indicators.trend_follower import TrendFollower
from indicators.trend_stability import TrendStability


def combine_momentum(momentum):
    return (1 + momentum).product() - 1


# NOTE: This is an "application" indicator only for analysis porpoises.


class FibTrader:
    def __init__(self, db: InfluxDBClient, exchange: str):
        frequency = timedelta(minutes=1)
        self.a = 3
        self.b = 2
        self.trend_follower = TrendFollower(db, exchange, a=self.a, b=self.b,
                                            frequency=frequency)
        periods = 5
        self.trend_stability = TrendStability(db, exchange, periods=periods,
                                              frequency=frequency)
        self.momentum = SlidingMomentum(db, exchange, 5, frequency)

    def compute(self) -> pd.Series:
        strength = self.trend_follower.compute()
        stability = self.trend_stability.compute()
        momentum = self.momentum.compute()
        overall = combine_momentum(momentum)
        a_mom, b_mom = momentum.iloc[:self.a], momentum.iloc[-self.b:]
        mask = (combine_momentum(a_mom) > 0.) & (combine_momentum(b_mom) > 0.)
        net_momentum = np.maximum(overall, 0.)
        net_strength = np.log2(np.maximum(strength, 1.))  # avoid zero division
        score = (net_strength * stability * net_momentum).loc[mask[mask].index]
        analysis = pd.DataFrame(
            {'strength': net_strength, 'stability': stability,
             'momentum': net_momentum, 'overall': score})
        analysis = analysis[analysis.overall > 0.]
        print(analysis.sort_values(by='overall', ascending=False))
        return score


def main(influx: InfluxDBClient):
    trader = FibTrader(influx, 'coinbasepro')
    while True:
        values = trader.compute()
        weights = values / values.sum()
        nonzero = weights[weights > 0]


if __name__ == '__main__':
    from settings import influx_db as influx_db_settings

    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    main(influx_client)
