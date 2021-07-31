from typing import Protocol

import pandas as pd


class RangeIndicator(Protocol):
    def compute(self, candles: pd.DataFrame, /) -> pd.DataFrame:
        ...


class InstantIndicator(Protocol):
    def compute(self, candles: pd.DataFrame, /) -> pd.Series:
        ...


class CandlesIndicator(Protocol):
    def compute(self) -> pd.DataFrame:
        """
        Returns DataFrame with 'market', 'time' index and ohlcv + quote_volume
        """
        pass


class BidAskIndicator(Protocol):
    def compute(self) -> pd.DataFrame:
        """Returns DataFrame with 'market' index and 'bid' and 'ask' columns"""
        pass
