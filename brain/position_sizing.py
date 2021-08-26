import random
from decimal import Decimal

import numpy as np
import pandas as pd

from helper.functions import overlapping_labels


def limit_limit_buy_size(spending_limit: float, weights: pd.Series,
                         prices: pd.Series,
                         min_sizes: pd.Series) -> pd.Series:
    total_weight = np.sum(weights)
    sorted_weights = weights.sort_values(ascending=False)
    max_above_limit, best_weights = 0, pd.Series([], dtype=np.float64)
    for market in sorted_weights.index:
        these_weights = sorted_weights.loc[:market]
        these_weights = these_weights / these_weights.sum() * total_weight
        these_amounts = these_weights * spending_limit
        these_sizes = these_amounts / prices
        these_sizes, min_sizes = overlapping_labels(these_sizes, min_sizes)
        large_enough = these_sizes > min_sizes
        these_amounts, large_enough = overlapping_labels(these_amounts,
                                                         large_enough)
        above_limit = these_amounts[large_enough]
        if len(above_limit) > max_above_limit:
            max_above_limit = len(above_limit)
            best_weights = above_limit / above_limit.sum() * total_weight
    return best_weights


def limit_market_buy_size(spending_limit: float, weights: pd.Series,
                          min_market_funds: pd.Series) -> pd.Series:
    total_weight = np.sum(weights)
    sorted_weights = weights.sort_values(ascending=False)
    max_above_limit, best_weights = 0, pd.Series([], dtype=np.float64)
    for market in sorted_weights.index:
        these_weights = sorted_weights.loc[:market]
        these_weights = these_weights / these_weights.sum() * total_weight
        these_amounts = these_weights * spending_limit
        these_amounts, min_market_funds = overlapping_labels(these_amounts,
                                                             min_market_funds)
        above_limit = these_amounts[these_amounts > min_market_funds]
        if len(above_limit) > max_above_limit:
            max_above_limit = len(above_limit)
            best_weights = above_limit / above_limit.sum() * total_weight
    return best_weights


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


__all__ = ['limit_limit_buy_size', 'limit_market_buy_size',
           'adjust_spending_target', 'compute_sell_size']
