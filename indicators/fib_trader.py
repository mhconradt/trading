import numpy as np
import pandas as pd

from indicators.acceleration import TrendAcceleration
from indicators.momentum import Momentum
from indicators.trend_stability import TrendStability
from indicators.volume import TrailingQuoteVolume


def combine_momentum(momentum):
    return (1 + momentum).product() - 1


class FibTrader:
    def __init__(self, a: int, b: int):
        self.a = a
        self.b = b
        self.periods = a + b
        self.acceleration = TrendAcceleration(a=self.a, b=self.b, trend_sign=1)
        self.stability = TrendStability(self.periods)
        self.momentum = Momentum(self.periods)
        self.quote_volume = TrailingQuoteVolume(self.periods)

    @property
    def periods_required(self) -> int:
        return max(self.acceleration.periods_required,
                   self.stability.periods_required,
                   self.momentum.periods_required,
                   self.quote_volume.periods_required)

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        strength = self.acceleration.compute(candles)
        stability = self.stability.compute(candles)
        momentum = self.momentum.compute(candles)
        volume = self.quote_volume.compute(candles)
        log_volume = np.log10(volume)
        most_recent_mom = momentum.iloc[-1]
        a_mom, b_mom = momentum.iloc[:self.a], momentum.iloc[-self.b:]
        mask = combine_momentum(a_mom) > 0.001
        net_momentum = np.maximum(most_recent_mom, 0.)
        net_strength = np.log2(np.maximum(strength, 1.))  # avoid zero division
        score = net_strength * stability * net_momentum * log_volume
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
    from indicators.sliding_candles import CandleSticks as SlidingCandles

    influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                            influx_db_settings.INFLUX_TOKEN,
                            org_id=influx_db_settings.INFLUX_ORG_ID,
                            org=influx_db_settings.INFLUX_ORG)

    trader = FibTrader(3, 2)
    records = []
    times = []
    sliding_indicator = SlidingCandles(influx, 'coinbasepro',
                                       trader.periods_required,
                                       timedelta(minutes=1))
    while True:
        _start = time.time()
        scores = trader.compute(sliding_indicator.compute())
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
