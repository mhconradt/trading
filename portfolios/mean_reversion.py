import logging
import signal
import sys
from datetime import timedelta

import numpy as np
import pandas as pd
from influxdb_client import InfluxDBClient

from brain.cool_down import CoolDown
from brain.portfolio_manager import PortfolioManager
from brain.stop_loss import SimpleStopLoss
from helper.coinbase import AuthenticatedClient
from indicators import TrailingVolume, TripleEMA, BidAsk, \
    Ticker
from indicators.sliding_candles import CandleSticks
from order_tracker.async_coinbase import AsyncCoinbaseTracker
from settings import influx_db as influx_db_settings, \
    coinbase as cb_settings, portfolio as portfolio_settings, \
    mean_reversion as strategy_settings

logging.basicConfig(format='%(levelname)s:%(module)s:%(message)s',
                    level=logging.DEBUG)

logger = logging.getLogger(__name__)


class MeanReversionBuy:
    def __init__(self, threshold: float, base_buy_fraction: float,
                 ema: TripleEMA):
        self.base_buy_fraction = base_buy_fraction
        self.threshold = threshold
        self.ema = ema

    @property
    def periods_required(self) -> int:
        return 1

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        prices = candles.close.unstack('market').iloc[-1]
        quote_volume = candles.quote_volume.unstack('market').iloc[-1]
        volume_fraction = quote_volume / quote_volume.sum()
        moving_averages = self.ema.compute()
        deviation = 1 - (prices / moving_averages)
        below = deviation > self.threshold
        adjustment = np.log(deviation[below] / self.threshold)
        hold_fraction_base = 1. - self.base_buy_fraction
        hold_fraction = hold_fraction_base ** adjustment
        buy_fraction = 1. - hold_fraction
        return (buy_fraction * volume_fraction)[below]


class MeanReversionSell:
    def __init__(self, threshold: float, base_sell_fraction: float,
                 ema: TripleEMA):
        self.base_sell_fraction = base_sell_fraction
        self.threshold = threshold
        self.ema = ema

    @property
    def periods_required(self) -> int:
        return 1

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        prices = candles.close.unstack('market').iloc[-1]
        moving_averages = self.ema.compute()
        deviations = prices / moving_averages - 1.
        above = deviations > self.threshold
        hold_fraction_base = 1. - self.base_sell_fraction
        # always >= 1.0
        acceleration = np.log(deviations[above] / self.threshold)
        # amount held geometrically decreases with the deviation
        hold_fraction = hold_fraction_base ** acceleration
        return 1. - hold_fraction


def main() -> None:
    influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                            influx_db_settings.INFLUX_TOKEN,
                            org_id=influx_db_settings.INFLUX_ORG_ID,
                            org=influx_db_settings.INFLUX_ORG)
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
    ema = TripleEMA(influx, strategy_settings.EMA_PERIODS,
                    strategy_settings.FREQUENCY,
                    portfolio_settings.QUOTE)
    buy_indicator = MeanReversionBuy(strategy_settings.BUY_THRESHOLD,
                                     strategy_settings.BASE_BUY_FRACTION, ema)
    sell_indicator = MeanReversionSell(strategy_settings.SELL_THRESHOLD,
                                       strategy_settings.BASE_SELL_FRACTION,
                                       ema)
    volume_indicator = TrailingVolume(periods=1)
    price_indicator = Ticker(periods=1)
    bid_ask = BidAsk(influx, period=timedelta(minutes=1),
                     bucket=strategy_settings.TICKER_BUCKET,
                     quote=portfolio_settings.QUOTE)
    candle_periods = max(buy_indicator.periods_required,
                         sell_indicator.periods_required,
                         volume_indicator.periods_required, )
    candles_src = CandleSticks(influx, portfolio_settings.EXCHANGE,
                               candle_periods, strategy_settings.FREQUENCY,
                               strategy_settings.TRADE_BUCKET,
                               portfolio_settings.QUOTE)
    buy_horizon = timedelta(seconds=strategy_settings.BUY_TARGET_SECONDS)
    sell_horizon = timedelta(seconds=strategy_settings.SELL_TARGET_SECONDS)
    # The idea here is to stop trading something after hitting the stop loss
    cool_down = CoolDown(sell_period=timedelta(hours=1))
    stop_loss = SimpleStopLoss(stop_loss=portfolio_settings.STOP_LOSS)
    manager = PortfolioManager(coinbase, candles_src,
                               buy_indicator=buy_indicator,
                               sell_indicator=sell_indicator,
                               price_indicator=price_indicator,
                               volume_indicator=volume_indicator,
                               bid_ask_indicator=bid_ask,
                               market_blacklist={'USDT-USD', 'DAI-USD',
                                                 'CLV-USD', 'PAX-USD'},
                               liquidate_on_shutdown=False,
                               quote=portfolio_settings.QUOTE,
                               order_tracker=tracker, cool_down=cool_down,
                               stop_loss=stop_loss,
                               sell_target_horizon=sell_horizon,
                               buy_age_limit=timedelta(seconds=30),
                               sell_age_limit=timedelta(seconds=30),
                               post_only=True, sell_order_type='limit',
                               buy_order_type='limit',
                               buy_target_horizon=buy_horizon)
    signal.signal(signal.SIGTERM, lambda _, __: manager.shutdown())
    try:
        manager.run()
    finally:
        manager.shutdown()
    sys.exit(1)


if __name__ == '__main__':
    main()
