from typing import Protocol

import pandas as pd

from .candles import CandleSticks
from .momentum import Momentum
from .moonshot import MoonShot, PessimisticMoonShot
from .ticker import Ticker


class RangeIndicator(Protocol):
    def compute(self) -> pd.DataFrame:
        ...


class InstantIndicator(Protocol):
    def compute(self) -> pd.Series:
        ...
