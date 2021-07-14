import logging
import signal
import sys
from datetime import timedelta

import numpy as np
import pandas as pd
from influxdb_client import InfluxDBClient

from brain.portfolio_manager import PortfolioManager
from brain.stop_loss import SimpleStopLoss
from brain.volatility_cooldown import VolatilityCoolDown
from helper.coinbase import AuthenticatedClient
from indicators import Ticker, TrailingVolume, TrendAcceleration, \
    TrendStability, Momentum
from indicators.fib_trader import FibTrader
from indicators.sliding_candles import CandleSticks
from settings import influx_db as influx_db_settings, \
    coinbase as coinbase_settings, portfolio as portfolio_settings

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
        self.score_moving_average *= self.alpha
        self.score_moving_average += (1 - self.alpha) * score_sum
        weights = scores / max(self.score_moving_average, score_sum)
        stability_adjusted_weights = weights * stability
        return stability_adjusted_weights.dropna()


class SellIndicator:
    def __init__(self, a: int, b: int):
        self.acceleration = TrendAcceleration(a, b, momentum_mode='close')
        self.stability = TrendStability(a + b)
        self.k = 1

    @property
    def periods_required(self) -> int:
        return max(self.acceleration.periods_required,
                   self.stability.periods_required)

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        stability = self.stability.compute(candles)
        acceleration = self.acceleration.compute(candles)
        fraction = np.maximum(0., np.minimum(1., (acceleration - 1) / -2))
        return (fraction * (self.k + stability)).fillna(1.)


def main() -> None:
    client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                            influx_db_settings.INFLUX_TOKEN,
                            org_id=influx_db_settings.INFLUX_ORG_ID,
                            org=influx_db_settings.INFLUX_ORG)
    coinbase = AuthenticatedClient(key=coinbase_settings.API_KEY,
                                   b64secret=coinbase_settings.SECRET,
                                   passphrase=coinbase_settings.PASSPHRASE,
                                   api_url=coinbase_settings.API_URL)
    stop_loss = SimpleStopLoss(take_profit=portfolio_settings.TAKE_PROFIT,
                               stop_loss=portfolio_settings.STOP_LOSS)
    cool_down = VolatilityCoolDown(buy_period=timedelta(minutes=0))
    buy_indicator = BuyIndicator(A, B)
    sell_indicator = SellIndicator(A, B)
    volume_indicator = TrailingVolume(periods=A + B)
    price_indicator = Ticker(A + B)
    candle_periods = max(buy_indicator.periods_required,
                         sell_indicator.periods_required,
                         price_indicator.periods_required,
                         volume_indicator.periods_required, )
    candles_src = CandleSticks(client, portfolio_settings.EXCHANGE,
                               candle_periods,
                               timedelta(minutes=1))
    manager = PortfolioManager(coinbase, candles_src,
                               buy_indicator=buy_indicator,
                               sell_indicator=sell_indicator,
                               price_indicator=price_indicator,
                               volume_indicator=volume_indicator,
                               cool_down=cool_down, liquidate_on_shutdown=True,
                               market_blacklist={'USDT-USD', 'DAI-USD'},
                               stop_loss=stop_loss, sell_order_type='market',
                               buy_order_type='market',
                               buy_time_in_force='FOK')
    signal.signal(signal.SIGTERM, lambda _, __: manager.shutdown())
    try:
        manager.run()
    finally:
        manager.shutdown()
    sys.exit(1)


if __name__ == '__main__':
    main()
