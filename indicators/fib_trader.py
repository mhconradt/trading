import numpy as np
import pandas as pd

from indicators.momentum import Momentum
from indicators.trend_follower import TrendFollower
from indicators.trend_stability import TrendStability
from indicators.volume import TrailingQuoteVolume


def combine_momentum(momentum):
    return (1 + momentum).product() - 1


class FibTrader:
    def __init__(self, a: int, b: int):
        self.a = a
        self.b = b
        self.periods = 5
        self.acceleration = TrendFollower(a=self.a, b=self.b, trend_sign=1)
        self.stability = TrendStability(self.periods)
        self.momentum = Momentum(self.periods)
        self.quote_volume = TrailingQuoteVolume(self.periods)

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        strength = self.acceleration.compute(candles)
        stability = self.stability.compute(candles)
        momentum = self.momentum.compute(candles)
        volume = self.quote_volume.compute(candles)
        log_volume = np.log10(volume)
        overall = combine_momentum(momentum)
        a_mom, b_mom = momentum.iloc[:self.a], momentum.iloc[-self.b:]
        mask = combine_momentum(a_mom) > 0.001
        net_momentum = np.maximum(overall, 0.)
        net_strength = np.log2(np.maximum(strength, 1.))  # avoid zero division
        score = (net_strength * stability * net_momentum * log_volume)
        score = score.loc[mask[mask].index]
        analysis = pd.DataFrame(
            {'strength': net_strength, 'stability': stability,
             'momentum': net_momentum, 'overall': score,
             'quote_volume': volume})
        analysis = analysis[analysis.overall > 0.]
        print(analysis.sort_values(by='overall', ascending=False).head(10))
        return score


def main():
    from datetime import datetime, timedelta
    import time

    import matplotlib.pyplot as plt
    from dateutil import tz
    from influxdb_client import InfluxDBClient

    from settings import influx_db as influx_db_settings
    from indicators.sliding_candles import CandleSticks

    influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                            influx_db_settings.INFLUX_TOKEN,
                            org_id=influx_db_settings.INFLUX_ORG_ID,
                            org=influx_db_settings.INFLUX_ORG)

    trader = FibTrader(3, 2)
    records = []
    times = []
    candles_indicator = CandleSticks(influx, 'coinbasepro', 6,
                                     timedelta(minutes=1))
    while True:
        _start = time.time()
        scores = trader.compute(candles_indicator.compute())
        nonzero = scores[scores > 0].rename('scores')
        record = nonzero.to_dict()
        if record:
            t = datetime.now(tz.UTC)
            times.append(t)
            records.append(record)
            df = pd.DataFrame(records, times)
            df.fillna(0.).plot(legend=False)
            plt.show()


if __name__ == '__main__':
    main()
