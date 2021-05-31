from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import typing as t

import pandas as pd
from pandas import DataFrame


class AbstractCandleCollector(ABC):
    @abstractmethod
    def get_market(self, market: str, start: datetime, end: datetime,
                   frequency: timedelta) -> DataFrame:
        """
        Fetch an interval of candlestick data for a single market
        :param market: The ID of the market i.e. ETH-USD.
        :param start: Inclusive start of interval.
        :param end: Exclusive end of interval.
        :param frequency: Frequency of the candlesticks to retrieve.
        :return: Pandas DataFrame with time index and ohlcv columns.
        """

    @abstractmethod
    def get_markets(self, markets: t.List[str], start: datetime, end: datetime,
                    frequency: timedelta) -> DataFrame:
        """
        Fetch an interval of candlestick data for a single market
        :param markets: The IDs of the market i.e. ETH-USD.
        :param start: Inclusive start of interval.
        :param end: Exclusive end of interval.
        :param frequency: Frequency of the candlesticks to retrieve.
        :return: Pandas DataFrame with market + time index and ohlcv columns.
        """


class CoinbaseCandleCollector(AbstractCandleCollector):
    def __init__(self):
        import cbpro
        self.client = cbpro.PublicClient()

    def clean_market_data(self, df: DataFrame,
                          frequency: timedelta) -> DataFrame:
        return df.resample(frequency).asfreq(pd.NA).sort_index()

    def get_market_interval(self, market: str, start: datetime, end: datetime,
                            frequency: timedelta) -> DataFrame:
        granularity = int(frequency.total_seconds())
        rows = self.client.get_product_historic_rates(market,
                                                      start.isoformat(),
                                                      end.isoformat(),
                                                      granularity)
        columns = ['time', 'low', 'high', 'open', 'close', 'volume']
        df = DataFrame(rows, columns=columns)
        df['time'] = pd.to_datetime(df['time'], unit='s')
        return df.set_index('time')

    def get_market(self, market: str, start: datetime, end: datetime,
                   frequency: timedelta) -> DataFrame:
        sub_intervals = pd.period_range(start, end, freq=300 * frequency)
        frames = []
        for interval in sub_intervals:
            far_right = end - datetime.resolution
            sub_df = self.get_market_interval(market, interval.start_time,
                                              min(interval.end_time,
                                                  far_right),
                                              frequency)
            frames.append(sub_df)
        df = pd.concat(frames)
        return self.clean_market_data(df, frequency)

    def get_markets(self, markets: t.List[str], start: datetime, end: datetime,
                    frequency: timedelta) -> DataFrame:
        sub_frames = [self.get_market(market, start, end, frequency)
                      for market in markets]
        df = pd.concat(sub_frames, keys=markets, names=['market', 'time'])
        return df.sort_index()
