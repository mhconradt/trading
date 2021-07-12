from typing import Protocol

import pandas as pd


class RangeIndicator(Protocol):
    def compute(self, candles: pd.DataFrame) -> pd.DataFrame:
        ...


class InstantIndicator(Protocol):
    def compute(self, candles: pd.DataFrame) -> pd.Series:
        ...
