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
        self.threshold = 0.001
        self.periods = periods
        self.ema = TripleEMA(db, periods, frequency)
        self.split_quote_volume = SplitQuoteVolume(db, 1, frequency)

    @property
    def periods_required(self) -> int:
        return 1

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        prices = candles.close.unstack('market').iloc[-1]
        quote_volume = self.split_quote_volume.compute()
        up_volume, down_volume = quote_volume['sell'], quote_volume['buy']
        moving_averages = self.ema.compute()
        deviation = 1 - (prices / moving_averages)
        below = deviation > self.threshold
        ape_index = down_volume / (down_volume + up_volume)
        ape_volume = down_volume * ape_index
        down_flow_fraction = ape_volume / ape_volume.sum()
        markets = down_flow_fraction.index.intersection(below.index)
        down_flow_fraction, below = down_flow_fraction[markets], below[markets]
        adjustment = deviation[markets] / self.threshold
        buy_targets = 1. - (1. - down_flow_fraction[below]) ** adjustment
        return buy_targets


class MeanReversionSell:
    def __init__(self, db: InfluxDBClient, periods: int = 26,
                 frequency: timedelta = timedelta(minutes=1)):
        self.threshold = 0.001
        self.periods = periods
        self.ema = TripleEMA(db, periods, frequency)

    @property
    def periods_required(self) -> int:
        return 1

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        prices = candles.close.unstack('market').iloc[-1]
        moving_averages = self.ema.compute()
        deviations = prices / moving_averages - 1.
        above = deviations > self.threshold
        sell_markets = above[above].index
        # x = y = 1/2 is the solution for x + y = 1 and x = y
        hold_fraction = np.ones_like(sell_markets) / 2.
        # always >= 1.0
        adjustment = deviations[sell_markets] / self.threshold
        # increase the rate of selling with larger deviations
        return 1. - hold_fraction ** adjustment


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
    bid_ask = BidAsk(client, period=timedelta(minutes=1))
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
                               market_blacklist={'USDT-USD', 'DAI-USD',
                                                 'CLV-USD', 'PAX-USD'},
                               stop_loss=stop_loss,
                               liquidate_on_shutdown=False,
                               buy_order_type='limit', sell_order_type='limit',
                               buy_target_horizon=timedelta(minutes=10),
                               sell_target_horizon=timedelta(minutes=10),
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
