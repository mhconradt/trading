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

TRADE_BUCKET = 'level1'

TICKER_BUCKET = 'level1'

FREQUENCY = timedelta(minutes=1)

EMA_PERIODS = 26

logging.basicConfig(format='%(levelname)s:%(module)s:%(message)s',
                    level=logging.DEBUG)

logger = logging.getLogger(__name__)


class MeanReversionBuy:
    def __init__(self, db: InfluxDBClient, periods: int = 26,
                 frequency: timedelta = timedelta(minutes=1)):
        self.threshold = 0.001
        self.periods = periods
        self.ema = TripleEMA(db, periods, frequency,
                             portfolio_settings.QUOTE)
        self.split_quote_volume = SplitQuoteVolume(db, 1, frequency,
                                                   TRADE_BUCKET,
                                                   portfolio_settings.QUOTE)

    @property
    def periods_required(self) -> int:
        return 1

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        prices = candles.close.unstack('market').iloc[-1]
        quote_volume = self.split_quote_volume.compute()
        up_volume, down_volume = quote_volume['sell'], quote_volume['buy']
        total_volume = up_volume + down_volume
        ape_index = down_volume / total_volume
        volume_fraction = total_volume / total_volume.sum()
        moving_averages = self.ema.compute()
        deviation = 1 - (prices / moving_averages)
        below = deviation > self.threshold
        markets = below[below].index.intersection(quote_volume.index)
        adjustment = np.log(deviation[markets] / self.threshold)
        hold_fraction = 1. - ape_index[markets]
        hold_fraction = hold_fraction ** adjustment
        buy_fraction = 1. - hold_fraction
        return buy_fraction * volume_fraction[markets]


class MeanReversionSell:
    def __init__(self, db: InfluxDBClient, periods: int = 26,
                 frequency: timedelta = timedelta(minutes=1)):
        self.threshold = 0.001
        self.periods = periods
        self.ema = TripleEMA(db, periods, frequency,
                             portfolio_settings.QUOTE)
        self.split_quote_volume = SplitQuoteVolume(db, 1, frequency,
                                                   TRADE_BUCKET,
                                                   portfolio_settings.QUOTE)

    @property
    def periods_required(self) -> int:
        return 1

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        prices = candles.close.unstack('market').iloc[-1]
        moving_averages = self.ema.compute()
        quote_volume = self.split_quote_volume.compute()
        up_volume, down_volume = quote_volume['sell'], quote_volume['buy']
        ape_index = up_volume / (up_volume + down_volume)
        deviations = prices / moving_averages - 1.
        above = deviations > self.threshold
        sell_markets = above[above].index.intersection(ape_index.index)
        # sell more quickly if it's quite apish
        hold_fraction = 1. - np.maximum(0.5, ape_index[sell_markets])
        # always >= 1.0
        acceleration = np.log(deviations[sell_markets] / self.threshold)
        # increase the rate of selling with larger deviations
        return 1. - hold_fraction ** acceleration


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
                               bid_ask_indicator=bid_ask, cool_down=cool_down,
                               market_blacklist={'USDT-USD', 'DAI-USD',
                                                 'CLV-USD', 'PAX-USD'},
                               stop_loss=stop_loss,
                               liquidate_on_shutdown=False,
                               quote=portfolio_settings.QUOTE,
                               buy_order_type='limit', sell_order_type='limit',
                               buy_target_horizon=timedelta(minutes=5),
                               sell_target_horizon=timedelta(minutes=5),
                               buy_age_limit=timedelta(seconds=15),
                               sell_age_limit=timedelta(seconds=15),
                               post_only=True)
    signal.signal(signal.SIGTERM, lambda _, __: manager.shutdown())
    try:
        manager.run()
    finally:
        manager.shutdown()
    sys.exit(1)


if __name__ == '__main__':
    main()
