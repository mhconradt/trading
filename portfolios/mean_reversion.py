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
from indicators import Ticker, TrailingVolume
from indicators.candles import CandleSticks
from indicators.ema import TripleEMA
from indicators.volume import SplitQuoteVolume
from settings import influx_db as influx_db_settings, \
    coinbase as coinbase_settings, portfolio as portfolio_settings

B = 2

A = 3

logging.basicConfig(format='%(levelname)s:%(module)s:%(message)s',
                    level=logging.DEBUG)


class MeanReversionBuy:
    def __init__(self, db: InfluxDBClient, periods: int = 26,
                 frequency: timedelta = timedelta(minutes=1)):
        self.threshold = 0.001
        self.periods = periods
        self.ema = TripleEMA(db, periods, frequency)
        self.split_quote_volume = SplitQuoteVolume(db, 5, frequency, 'buy')

    @property
    def periods_required(self) -> int:
        return 0

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        prices = candles.close.unstack('market').iloc[-1]
        down_quote_volume = self.split_quote_volume.compute()
        moving_averages = self.ema.compute()
        below = 1 - (prices / moving_averages) > self.threshold
        volume_fractions = down_quote_volume / down_quote_volume.sum()
        return volume_fractions[below]


class MeanReversionSell:
    def __init__(self, db: InfluxDBClient, periods: int = 26,
                 frequency: timedelta = timedelta(minutes=1)):
        self.threshold = 0.001
        self.periods = periods
        self.ema = TripleEMA(db, periods, frequency)

    @property
    def periods_required(self) -> int:
        return 0

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        prices = candles.close.unstack('market').iloc[-1]
        moving_averages = self.ema.compute()
        above = moving_averages / prices - 1. > self.threshold
        sells = above[above].index
        return pd.Series(np.ones_like(sells), sells)


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
    buy_indicator = MeanReversionBuy(client, periods=26,
                                     frequency=timedelta(minutes=1))
    sell_indicator = MeanReversionSell(client, periods=26,
                                       frequency=timedelta(minutes=1))
    volume_indicator = TrailingVolume(periods=5)
    price_indicator = Ticker(A + B)
    candle_periods = max(buy_indicator.periods_required,
                         sell_indicator.periods_required,
                         price_indicator.periods_required,
                         volume_indicator.periods_required, )
    candles_src = CandleSticks(client, portfolio_settings.EXCHANGE,
                               candle_periods,
                               timedelta(minutes=1), offset=0)
    manager = PortfolioManager(coinbase, candles_src,
                               buy_indicator=buy_indicator,
                               sell_indicator=sell_indicator,
                               price_indicator=price_indicator,
                               volume_indicator=volume_indicator,
                               cool_down=cool_down, liquidate_on_shutdown=True,
                               market_blacklist={'USDT-USD', 'DAI-USD'},
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
