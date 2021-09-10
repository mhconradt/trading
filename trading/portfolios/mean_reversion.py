import logging
import signal
import sys
import time
from datetime import timedelta

import numpy as np
import pandas as pd
from influxdb_client import InfluxDBClient

from trading.brain.cool_down import CoolDown
from trading.brain.portfolio_manager import PortfolioManager
from trading.brain.stop_loss import SimpleStopLoss
from trading.coinbase.helper import AuthenticatedClient
from trading.helper.functions import min_max, overlapping_labels
from trading.indicators import (ATR, BidAsk, MarketFraction, RelativeMMI,
                                Ticker, TrailingVolume, TripleEMA)
from trading.indicators.sliding_candles import CandleSticks
from trading.order_tracker.async_coinbase import AsyncCoinbaseTracker
from trading.settings import mean_reversion as strategy_settings, \
    portfolio as portfolio_settings, coinbase as cb_settings, \
    influx_db as influx_db_settings

logging.basicConfig(format='%(levelname)s:%(module)s:%(message)s',
                    level=logging.DEBUG)

logger = logging.getLogger(__name__)


def combine_signals(mean_reversion: np.array, rmmi: np.array) -> np.array:
    # anything between cancelling out and doubling effect.
    trimmed_rmmi = min_max(-mean_reversion, rmmi, mean_reversion)
    return mean_reversion + trimmed_rmmi


class MeanReversionBuy:
    def __init__(self, base_buy_fraction: float, ema: TripleEMA, atr: ATR,
                 market_fraction: MarketFraction, rmmi: RelativeMMI):
        self.base_buy_fraction = base_buy_fraction
        self.ema = ema
        self.atr = atr
        self.market_fraction = market_fraction
        self.rmmi = rmmi

    @property
    def periods_required(self) -> int:
        return 1

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        price = candles.close.unstack('market').iloc[-1]
        market_fraction = self.market_fraction.compute()
        moving_average = self.ema.compute()
        atr = self.atr.compute()
        rmmi = self.rmmi.compute()
        deviation = moving_average - price
        threshold = atr / 2.
        deviation, threshold = overlapping_labels(deviation, threshold)
        below = deviation > threshold
        reversion_acceleration = np.log(deviation[below] / threshold[below])
        acceleration = combine_signals(reversion_acceleration, -rmmi)
        hold_fraction_base = 1. - self.base_buy_fraction
        hold_fraction = hold_fraction_base ** acceleration
        buy_fraction = 1. - hold_fraction
        # market only present in buy fraction if exceeds threshold
        return (buy_fraction * market_fraction).dropna()


class MeanReversionSell:
    def __init__(self, base_sell_fraction: float, ema: TripleEMA, atr: ATR,
                 rmmi: RelativeMMI):
        self.base_sell_fraction = base_sell_fraction
        self.ema = ema
        self.atr = atr
        self.rmmi = rmmi

    @property
    def periods_required(self) -> int:
        return 1

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        price = candles.close.unstack('market').iloc[-1]
        moving_average = self.ema.compute()
        atr = self.atr.compute()
        rmmi = self.rmmi.compute()
        deviation = price - moving_average
        threshold = atr / 2.
        deviation, threshold = overlapping_labels(deviation, threshold)
        above = deviation > threshold
        hold_fraction_base = 1. - self.base_sell_fraction
        # always >= 1.0
        reversion_acceleration = np.log(deviation[above] / threshold[above])
        acceleration = combine_signals(reversion_acceleration, rmmi)
        # amount held geometrically decreases with the deviation
        hold_fraction = hold_fraction_base ** acceleration
        return 1. - hold_fraction


def main() -> None:
    influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                            influx_db_settings.INFLUX_TOKEN,
                            org_id=influx_db_settings.INFLUX_ORG_ID,
                            org=influx_db_settings.INFLUX_ORG)
    ema = TripleEMA(influx, strategy_settings.EMA_PERIODS,
                    strategy_settings.FREQUENCY,
                    portfolio_settings.QUOTE)
    atr = ATR(influx, periods=14, frequency=strategy_settings.FREQUENCY,
              quote=portfolio_settings.QUOTE)
    market_fraction = MarketFraction(influx, periods=60,
                                     frequency=strategy_settings.FREQUENCY,
                                     quote=portfolio_settings.QUOTE)
    rmmi = RelativeMMI(influx, market_fraction,
                       period=strategy_settings.RMMI_PERIOD,
                       toleration=timedelta(seconds=60),
                       quote=portfolio_settings.QUOTE)
    buy_indicator = MeanReversionBuy(strategy_settings.BASE_BUY_FRACTION, ema,
                                     atr, market_fraction, rmmi)
    sell_indicator = MeanReversionSell(strategy_settings.BASE_SELL_FRACTION,
                                       ema, atr, rmmi)
    volume_indicator = TrailingVolume(periods=1)
    price_indicator = Ticker(periods=1)
    bid_ask = BidAsk(influx, period=timedelta(minutes=1),
                     bucket=strategy_settings.TICKER_BUCKET,
                     quote=portfolio_settings.QUOTE)
    candle_periods = max(buy_indicator.periods_required,
                         sell_indicator.periods_required,
                         volume_indicator.periods_required, )
    candles_src = CandleSticks(influx, candle_periods,
                               strategy_settings.FREQUENCY,
                               strategy_settings.TRADE_BUCKET,
                               portfolio_settings.QUOTE)
    # The idea here is to stop trading something after hitting the stop loss
    cool_down = CoolDown(sell_period=portfolio_settings.STOP_LOSS_COOLDOWN)
    stop_loss = SimpleStopLoss(stop_loss=portfolio_settings.STOP_LOSS)
    while True:
        outer_tick_start = time.time()
        coinbase = AuthenticatedClient(key=cb_settings.API_KEY,
                                       b64secret=cb_settings.SECRET,
                                       passphrase=cb_settings.PASSPHRASE,
                                       api_url=cb_settings.API_URL)
        products = [product['id'] for product in coinbase.get_products() if
                    product['quote_currency'] == portfolio_settings.QUOTE]
        tracker = AsyncCoinbaseTracker(products=products,
                                       api_key=cb_settings.API_KEY,
                                       api_secret=cb_settings.SECRET,
                                       api_passphrase=cb_settings.PASSPHRASE,
                                       ignore_untracked=False)
        min_tick_time = portfolio_settings.MIN_TICK_TIME
        concentration_limit = portfolio_settings.CONCENTRATION_LIMIT
        manager = PortfolioManager(coinbase, candles_src,
                                   buy_indicator=buy_indicator,
                                   sell_indicator=sell_indicator,
                                   price_indicator=price_indicator,
                                   volume_indicator=volume_indicator,
                                   bid_ask_indicator=bid_ask,
                                   market_blacklist={'USDT-USD', 'DAI-USD',
                                                     'CLV-USD', 'PAX-USD', },
                                   liquidate_on_shutdown=False,
                                   quote=portfolio_settings.QUOTE,
                                   order_tracker=tracker,
                                   cool_down=cool_down,
                                   stop_loss=stop_loss,
                                   sell_horizon=strategy_settings.SELL_HORIZON,
                                   buy_age_limit=timedelta(seconds=30),
                                   sell_age_limit=timedelta(seconds=30),
                                   post_only=True, sell_order_type='limit',
                                   buy_order_type='limit',
                                   buy_horizon=strategy_settings.BUY_HORIZON,
                                   min_tick_time=min_tick_time,
                                   concentration_limit=concentration_limit)
        signal.signal(signal.SIGTERM, lambda _, __: manager.shutdown())
        try:
            manager.run()
        except KeyboardInterrupt:
            break
        except (Exception,):
            if time.time() - outer_tick_start < 60:
                break
        finally:
            externally_killed = manager.stop
            manager.shutdown()
            if externally_killed:
                break
    sys.exit(1)


if __name__ == '__main__':
    main()
