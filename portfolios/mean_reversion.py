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
from indicators import TrailingVolume, SplitQuoteVolume, TripleEMA, BidAsk, \
    Ticker
from indicators.sliding_candles import CandleSticks
from settings import influx_db as influx_db_settings, \
    coinbase as coinbase_settings, portfolio as portfolio_settings

FREQUENCY = timedelta(minutes=1)

EMA_PERIODS = 26

logging.basicConfig(format='%(levelname)s:%(module)s:%(message)s',
                    level=logging.DEBUG)


class MeanReversionBuy:
    def __init__(self, db: InfluxDBClient, periods: int = 26,
                 frequency: timedelta = timedelta(minutes=1)):
        self.threshold = 0.002
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
        self.threshold = 0.002
        self.periods = periods
        self.ema = TripleEMA(db, periods, frequency)

    @property
    def periods_required(self) -> int:
        return 0

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        prices = candles.close.unstack('market').iloc[-1]
        moving_averages = self.ema.compute()
        above = prices / moving_averages - 1. > self.threshold
        return above.astype(np.float64)


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
    buy_indicator = MeanReversionBuy(client, periods=EMA_PERIODS,
                                     frequency=FREQUENCY)
    sell_indicator = MeanReversionSell(client, periods=EMA_PERIODS,
                                       frequency=FREQUENCY)
    volume_indicator = TrailingVolume(periods=5)
    price_indicator = Ticker(periods=5)
    bid_ask = BidAsk(client, period=timedelta(minutes=5))
    candle_periods = max(buy_indicator.periods_required,
                         sell_indicator.periods_required,
                         volume_indicator.periods_required, )
    candles_src = CandleSticks(client, portfolio_settings.EXCHANGE,
                               candle_periods,
                               FREQUENCY, offset=0)
    manager = PortfolioManager(coinbase, candles_src,
                               buy_indicator=buy_indicator,
                               sell_indicator=sell_indicator,
                               price_indicator=price_indicator,
                               volume_indicator=volume_indicator,
                               bid_ask_indicator=bid_ask, cool_down=cool_down,
                               market_blacklist={'USDT-USD', 'DAI-USD'},
                               stop_loss=stop_loss,
                               liquidate_on_shutdown=False,
                               buy_order_type='limit',
                               sell_order_type='limit',
                               buy_half_life=timedelta(minutes=3),
                               sell_half_life=timedelta(minutes=3),
                               post_only=True,
                               buy_age_limit=timedelta(seconds=15),
                               sell_age_limit=timedelta(seconds=30))
    signal.signal(signal.SIGTERM, lambda _, __: manager.shutdown())
    try:
        manager.run()
    finally:
        manager.shutdown()
    sys.exit(1)


if __name__ == '__main__':
    main()
