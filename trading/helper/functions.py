import typing as t
from decimal import Decimal

import pandas as pd


def overlapping_labels(a: pd.Series,
                       b: pd.Series) -> t.Tuple[pd.Series, pd.Series]:
    intersection = a.index.intersection(b.index)
    return a.loc[intersection], b.loc[intersection]


def safely_decimalize(s: pd.Series) -> pd.Series:
    return s.map(Decimal).where(s.notna(), pd.NA)
