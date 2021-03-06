import typing as t
from decimal import Decimal

import numpy as np
import pandas as pd


def overlapping_labels(a: pd.Series,
                       b: pd.Series) -> t.Tuple[pd.Series, pd.Series]:
    intersection = a.index.intersection(b.index)
    return a.loc[intersection], b.loc[intersection]


def min_max(minimum: t.Union[float, np.array], a: np.array,
            maximum: t.Union[float, np.array]) -> np.array:
    return np.maximum(np.minimum(a, maximum), minimum)


def safely_decimalize(s: pd.Series) -> pd.Series:
    return s.map(Decimal).where(s.notna(), pd.NA)
