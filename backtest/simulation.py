import sys
import time

import numpy as np
from numba import njit

FLOAT_INFO_EPSILON = sys.float_info.epsilon


@njit
def simulate(fiat: float, buy_fraction: np.array, sell_fraction: np.array,
             price: np.array, fee: float, buy_expiration: int = 30,
             sell_expiration: int = 30,
             single_trade: bool = False) -> np.array:
    """
    Simulate a portfolio's trading activity + returns
    :param single_trade: do not buy more once a position is on
    :param fiat: the starting fiat balance
    :param buy_fraction: [t, m] = fraction of fiat to spend on m at t
    :param sell_fraction: [t, m] = fraction of balance[m] to sell at t
    :param price: [t, m] = price of m at t
    :param fee: fee paid to exchange as fraction of spending amount
    :param buy_expiration: the number of periods after which the buy expires
    :param sell_expiration: the number of periods after which the sell expires
    :return: the final market value of the portfolio
    """
    m = buy_fraction.shape[1]
    buy_sizes = np.zeros((buy_expiration, m))
    buy_prices = np.zeros((buy_expiration, m))
    sell_sizes = np.zeros((sell_expiration, m))
    sell_prices = np.zeros((sell_expiration, m))
    available_balance = np.zeros(m)
    total_balance = np.zeros(m)
    pending_buy_size = np.zeros(m)
    aum_tracker = np.zeros(buy_fraction.shape[0])
    most_recent_price = price[0]
    # buy weights to zero if there is a non-zero balance
    for t in range(buy_fraction.shape[0]):
        most_recent_price = np.where(np.isnan(price[t]),
                                     most_recent_price, price[t])
        order_price = np.where(np.isnan(most_recent_price),
                               1., most_recent_price)
        # holds -> balance
        buy_fills = buy_prices > price[t]  # filled if market moves below price
        fiat_fill_total = (buy_sizes * buy_fills * buy_prices).sum()
        fiat -= fiat_fill_total * fee
        filled_size = (buy_sizes * buy_fills).sum(axis=0)
        pending_buy_size -= filled_size
        available_balance += filled_size
        total_balance += filled_size
        buy_sizes *= ~buy_fills
        # holds -> fiat
        sell_fills = sell_prices < price[t]
        total_balance -= (sell_sizes * sell_fills).sum(axis=0)
        proceeds = (sell_sizes * sell_fills * sell_prices).sum()
        fiat += proceeds * (1 - fee)
        sell_sizes *= ~sell_fills
        # expiration
        retry = sell_fraction[t] > 0.
        retry_base_amount = retry * sell_sizes[t % sell_expiration]
        available_balance += ~retry * sell_sizes[t % sell_expiration]
        fiat += buy_sizes[t % buy_expiration] @ buy_prices[t % buy_expiration]
        pending_buy_size -= buy_sizes[t % buy_expiration]
        # buys -> holds
        buy_fraction_t = np.where(np.isnan(most_recent_price), 0.,
                                  buy_fraction[t])
        if single_trade:
            buy_fraction_t *= total_balance + pending_buy_size == 0.
        buy_quote_amount = fiat * buy_fraction_t
        fiat -= buy_quote_amount.sum()
        buy_base_amount = buy_quote_amount / order_price
        buy_sizes[t % buy_expiration] = buy_base_amount
        pending_buy_size += buy_base_amount
        buy_prices[t % buy_expiration] = order_price
        # sells -> holds
        sell_fraction_t = np.where(np.isnan(most_recent_price), 0.,
                                   sell_fraction[t])
        sell_base_amount = available_balance * sell_fraction_t
        available_balance -= sell_base_amount
        sell_sizes[t % sell_expiration] = sell_base_amount + retry_base_amount
        sell_prices[t % sell_expiration] = order_price
        cash_and_cash_equivalents = fiat + (buy_prices * buy_sizes).sum()
        m2m = (total_balance * order_price).sum()
        aum_tracker[t] = cash_and_cash_equivalents + m2m
        continue
    return aum_tracker


def generate_price(t: int) -> np.array:
    """
    Simulate log-normal price movements
    """
    moves = np.random.randn(t) * 1e-4 + 1e-7
    return np.exp(np.cumsum(moves))


def main() -> None:
    np.random.seed(42)
    m = 64
    t = 86400
    fee = 0.0008
    expiration = 10
    buy_fraction = (1 - (1 - np.random.rand(t, m)) ** (1 / 300)) / m
    # buy_fraction = np.ones((t, m)) / m
    sell_fraction = 1 - (1 - np.random.rand(t, m)) ** (1 / 300)
    # sell_fraction = np.zeros((t, m))
    price = np.hstack([np.expand_dims(generate_price(t), -1)
                       for _ in range(m)])
    drop = np.random.permutation(np.arange(price.size).reshape(*price.shape))
    price[drop % 7 == 0] = np.nan
    # numba warmup
    start = time.time()
    for i in range(7):
        print(simulate(100_000., buy_fraction, sell_fraction, price, fee,
                       expiration,
                       expiration))
    print(f"{time.time() - start:.2f}s")


if __name__ == '__main__':
    main()
