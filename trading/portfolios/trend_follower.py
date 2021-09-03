import logging
import signal
import sys
from datetime import timedelta

import numpy as np
import pandas as pd
from influxdb_client import InfluxDBClient

from trading.brain.cool_down import CoolDown
from trading.brain.portfolio_manager import PortfolioManager
from trading.brain.stop_loss import SimpleStopLoss
from trading.coinbase.helper import AuthenticatedClient
from trading.indicators import Ticker, TrailingVolume, TrendAcceleration, \
    TrendStability, Momentum, BidAsk
from trading.indicators.fib_trader import FibTrader
from trading.indicators.sliding_candles import CandleSticks
from trading.order_tracker import SyncCoinbaseTracker
from trading.settings import portfolio as portfolio_settings, \
    coinbase as coinbase_settings, influx_db as influx_db_settings

B = 2

A = 3

logging.basicConfig(format='%(levelname)s:%(module)s:%(message)s',
                    level=logging.DEBUG)


class BuyIndicator:
    def __init__(self, a: int, b: int):
        self.a = a
        self.b = b
        self.stability = TrendStability(periods=self.a + self.b)
        self.fib_trader = FibTrader(a, b)
        self.momentum = Momentum(periods=1, span=15)
        self.alpha = 0.995
        self.score_moving_average = 0.

    @property
    def periods_required(self) -> int:
        return max(self.fib_trader.periods_required,
                   self.stability.periods_required,
                   self.momentum.periods_required)

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        scores = self.fib_trader.compute(candles)
        stability = self.stability.compute(candles)
        roc = self.momentum.compute(candles).iloc[-1]
        pos_roc = roc[roc > 0.]
        scores = scores[scores > 0.]
        scores = scores.loc[scores.index.intersection(pos_roc.index)]
        score_sum = scores.sum()
        if score_sum > 0.:
            self.score_moving_average *= self.alpha
            self.score_moving_average += (1 - self.alpha) * score_sum
        weights = scores / max(self.score_moving_average, score_sum)
        stability_adjusted_weights = weights * stability
        return stability_adjusted_weights.dropna()


class SellIndicator:
    def __init__(self, a: int, b: int):
        self.acceleration = TrendAcceleration(a, b, momentum_mode='close')
        self.stability = TrendStability(a + b)

    @property
    def periods_required(self) -> int:
        return self.acceleration.periods_required

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        acceleration = self.acceleration.compute(candles)
        stability = self.stability.compute(candles)
        deceleration = np.maximum(0., np.minimum(2., -(acceleration - 1)))
        fraction = deceleration / 2
        instability = 1 - stability
        return (fraction * (1 + instability) / 2).fillna(1.)


def main() -> None:
    client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                            influx_db_settings.INFLUX_TOKEN,
                            org_id=influx_db_settings.INFLUX_ORG_ID,
                            org=influx_db_settings.INFLUX_ORG)
    coinbase = AuthenticatedClient(key=coinbase_settings.API_KEY,
                                   b64secret=coinbase_settings.SECRET,
                                   passphrase=coinbase_settings.PASSPHRASE,
                                   api_url=coinbase_settings.API_URL)
    tracker = SyncCoinbaseTracker(coinbase)
    cool_down = CoolDown(sell_period=timedelta(hours=1))
    stop_loss = SimpleStopLoss(stop_loss=portfolio_settings.STOP_LOSS)
    buy_indicator = BuyIndicator(A, B)
    sell_indicator = SellIndicator(A, B)
    volume_indicator = TrailingVolume(periods=A + B)
    price_indicator = Ticker(A + B)
    candle_periods = max(buy_indicator.periods_required,
                         sell_indicator.periods_required,
                         price_indicator.periods_required,
                         volume_indicator.periods_required, )
    candles_src = CandleSticks(client, candle_periods, timedelta(minutes=1),
                               'level1', portfolio_settings.QUOTE)
    bid_ask = BidAsk(client, period=timedelta(minutes=1), bucket='level1',
                     quote=portfolio_settings.QUOTE)
    manager = PortfolioManager(coinbase, candles_src,
                               buy_indicator=buy_indicator,
                               sell_indicator=sell_indicator,
                               price_indicator=price_indicator,
                               volume_indicator=volume_indicator,
                               bid_ask_indicator=bid_ask,
                               market_blacklist={'USDT-USD', 'DAI-USD'},
                               liquidate_on_shutdown=True,
                               quote=portfolio_settings.QUOTE,
                               order_tracker=tracker, cool_down=cool_down,
                               stop_loss=stop_loss, sell_order_type='market',
                               buy_order_type='market')
    signal.signal(signal.SIGTERM, lambda _, __: manager.shutdown())
    try:
        manager.run()
    finally:
        manager.shutdown()
    sys.exit(1)


if __name__ == '__main__':
    main()
