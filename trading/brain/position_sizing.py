import random
import typing as t
from decimal import Decimal

import numpy as np
import pandas as pd

from trading.helper.functions import overlapping_labels


def limit_limit_buy_amounts(amounts: pd.Series, prices: pd.Series,
                            min_sizes: pd.Series,
                            probabilistic: bool = False) -> pd.Series:
    if probabilistic:
        return probabilistic_limit_buy_amounts(amounts, prices, min_sizes)
    else:
        return deterministic_limit_buy_amounts(amounts, prices, min_sizes)


def deterministic_limit_buy_amounts(amounts: pd.Series, prices: pd.Series,
                                    min_sizes: pd.Series) -> pd.Series:
    sizes = amounts / prices
    sizes, min_sizes = overlapping_labels(sizes, min_sizes)
    return (sizes[sizes > min_sizes] * prices).dropna()


def probabilistic_limit_buy_amounts(amounts: pd.Series, prices: pd.Series,
                                    min_sizes: pd.Series) -> pd.Series:
    sizes = amounts / prices
    sizes, min_sizes = overlapping_labels(sizes, min_sizes)
    p = sizes / min_sizes
    n = len(sizes)
    randomized_size = t.cast(np.array, np.random.rand(n) < p).astype(float)
    new_size = sizes.where(sizes >= min_sizes, randomized_size)
    return new_size * prices


def limit_market_buy_amounts(amounts: pd.Series,
                             min_market_funds: pd.Series,
                             probabilistic: bool = False) -> pd.Series:
    if probabilistic:
        return probabilistic_market_buy_amounts(amounts, min_market_funds)
    else:
        return deterministic_market_buy_amounts(amounts, min_market_funds)


def deterministic_market_buy_amounts(amounts: pd.Series,
                                     min_funds: pd.Series) -> pd.Series:
    amounts, min_funds = overlapping_labels(amounts, min_funds)
    return amounts[amounts > min_funds]


def probabilistic_market_buy_amounts(amounts: pd.Series,
                                     min_funds: pd.Series) -> pd.Series:
    amounts, min_funds = overlapping_labels(amounts, min_funds)
    p = amounts / min_funds
    n = len(amounts)
    randomized_funds = t.cast(np.array, np.random.rand(n) < p).astype(float)
    new_funds = amounts.where(amounts >= min_funds, randomized_funds)
    return new_funds


def _compute_sell_size1(size: Decimal, fraction: Decimal,
                        min_size: Decimal, increment: Decimal) -> Decimal:
    """
    Determine the size of the position to sell.
    This size must satisfy the following requirements:
        1. The size must obey exchange rules.
    :param size: the position size
    :param fraction: the desired fraction to sell
    :param min_size: the minimum size for an order
    :param increment: this is the minimum increment for order sizes.
    :return: the size to sell.
    """
    desired_size = fraction * size
    obeys_increment = desired_size.quantize(increment, rounding='ROUND_UP')
    if obeys_increment < min_size:
        # sell what you want in expectation
        sell_probability = float(obeys_increment / min_size)
        if random.random() < sell_probability:
            return min_size
        else:
            return Decimal('0')
    return obeys_increment


def compute_sell_size(size: Decimal, fraction: Decimal,
                      min_size: Decimal, increment: Decimal) -> Decimal:
    """
        Determine the size of the position to sell.
        This size must satisfy the following requirements:
            1. The size must obey exchange rules.
            2. The size should ensure the remaining size can be sold.
        :param size: the position size
        :param fraction: the desired fraction to sell
        :param min_size: the minimum size for an order
        :param increment: this is the minimum increment for order sizes.
        :return: the size to sell.
    """
    l1_sell_size = _compute_sell_size1(size, fraction, min_size,
                                       increment)
    if (size - l1_sell_size) < min_size:
        return size.quantize(increment, rounding='ROUND_DOWN')
    else:
        return l1_sell_size


def adjust_spending_target(targets: pd.Series,
                           over: pd.Series) -> pd.Series:
    exp = 1 / over
    weights = 1 - (1 - targets) ** exp
    return weights.fillna(0.).map(Decimal)


__all__ = ['limit_limit_buy_amounts', 'limit_market_buy_amounts',
           'adjust_spending_target', 'compute_sell_size']
