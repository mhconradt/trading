from typing import Protocol

import pandas as pd


class RangeIndicator(Protocol):
    def compute(self) -> pd.DataFrame:
        ...


class InstantIndicator(Protocol):
    def compute(self) -> pd.Series:
        ...
