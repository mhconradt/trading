import logging
import signal
import sys
from datetime import timedelta

import numpy as np
import pandas as pd
from influxdb_client import InfluxDBClient

from brain.portfolio_manager import PortfolioManager
from helper.coinbase import AuthenticatedClient
from indicators import TrailingVolume, TripleEMA, BidAsk, \
    Ticker
from indicators.sliding_candles import CandleSticks
from order_tracker.async_coinbase import AsyncCoinbaseTracker
from settings import influx_db as influx_db_settings, \
    coinbase as cb_settings, portfolio as portfolio_settings

# STRATEGY PARAMETERS
BUY_FRACTION_BASE = 1 / 2
SELL_FRACTION_BASE = 8 / 9

DEVIATION_THRESHOLD = 0.001

# we're in the moving AND intermediate storage biz

# try to more evenly distribute buys to let the price have more effect

TRADE_BUCKET = 'level1'

TICKER_BUCKET = 'level1'

FREQUENCY = timedelta(minutes=1)

EMA_PERIODS = 26

logging.basicConfig(format='%(levelname)s:%(module)s:%(message)s',
                    level=logging.DEBUG)

logger = logging.getLogger(__name__)


class MeanReversionBuy:
    def __init__(self, db: InfluxDBClient, periods: int = EMA_PERIODS,
                 frequency: timedelta = timedelta(minutes=1)):
        self.threshold = DEVIATION_THRESHOLD
        self.periods = periods
        self.ema = TripleEMA(db, periods, frequency,
                             portfolio_settings.QUOTE)

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
        hold_fraction_base = 1. - BUY_FRACTION_BASE
        hold_fraction = hold_fraction_base ** adjustment
        buy_fraction = 1. - hold_fraction
        return (buy_fraction * volume_fraction)[below]


class MeanReversionSell:
    def __init__(self, db: InfluxDBClient, periods: int = EMA_PERIODS,
                 frequency: timedelta = timedelta(minutes=1)):
        self.threshold = DEVIATION_THRESHOLD
        self.periods = periods
        self.ema = TripleEMA(db, periods, frequency,
                             portfolio_settings.QUOTE)

    @property
    def periods_required(self) -> int:
        return 1

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        prices = candles.close.unstack('market').iloc[-1]
        moving_averages = self.ema.compute()
        deviations = prices / moving_averages - 1.
        above = deviations > self.threshold
        hold_fraction_base = 1. - SELL_FRACTION_BASE
        # always >= 1.0
        acceleration = np.log(deviations[above] / self.threshold)
        # amount held geometrically decreases with the deviation
        hold_fraction = hold_fraction_base ** acceleration
        return 1. - hold_fraction


def main() -> None:
    client = InfluxDBClient(influx_db_settings.INFLUX_URL,
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
    buy_indicator = MeanReversionBuy(client, periods=EMA_PERIODS,
                                     frequency=FREQUENCY)
    sell_indicator = MeanReversionSell(client, periods=EMA_PERIODS,
                                       frequency=FREQUENCY)
    volume_indicator = TrailingVolume(periods=1)
    price_indicator = Ticker(periods=1)
    bid_ask = BidAsk(client, period=timedelta(minutes=1), bucket=TICKER_BUCKET,
                     quote=portfolio_settings.QUOTE)
    candle_periods = max(buy_indicator.periods_required,
                         sell_indicator.periods_required,
                         volume_indicator.periods_required, )
    candles_src = CandleSticks(client, portfolio_settings.EXCHANGE,
                               candle_periods, FREQUENCY, TRADE_BUCKET,
                               portfolio_settings.QUOTE)
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
                               order_tracker=tracker, buy_order_type='limit',
                               buy_target_horizon=timedelta(minutes=5),
                               sell_target_horizon=timedelta(minutes=5),
                               buy_age_limit=timedelta(seconds=15),
                               sell_age_limit=timedelta(seconds=30),
                               post_only=True, sell_order_type='limit')
    signal.signal(signal.SIGTERM, lambda _, __: manager.shutdown())
    try:
        manager.run()
    finally:
        manager.shutdown()
    sys.exit(1)


if __name__ == '__main__':
    main()
