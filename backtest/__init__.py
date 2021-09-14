import sys
import time

import numba
import numpy as np

FLOAT_INFO_EPSILON = sys.float_info.epsilon


@numba.njit
def simulate(fiat: float, buy_fraction: np.array, sell_fraction: np.array,
             price: np.array, fee: float, buy_expiration: int = 30,
             sell_expiration: int = 30) -> np.array:
    """
    Simulate a portfolio's trading activity + returns
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
    balance = np.zeros(m)
    order_price = np.full(m, FLOAT_INFO_EPSILON)
    for t in range(buy_fraction.shape[0]):
        order_price = np.where(~np.isnan(price[t]), price[t], order_price)
        # holds -> balance
        buy_fills = buy_prices > price[t]  # filled if market moves below price
        fiat_fill_total = (buy_sizes * buy_fills * buy_prices).sum()
        fiat -= fiat_fill_total * fee
        filled_size = (buy_sizes * buy_fills).sum(axis=0)
        balance += filled_size
        buy_sizes *= ~buy_fills
        # holds -> fiat
        sell_fills = sell_prices < price[t]
        proceeds = (sell_sizes * sell_fills * sell_prices).sum()
        fiat += proceeds * (1 - fee)
        sell_sizes *= ~sell_fills
        # expiration
        balance += sell_sizes[t % sell_expiration]
        fiat += buy_sizes[t % buy_expiration] @ buy_prices[t % buy_expiration]
        # buys -> holds
        buy_quote_amount = fiat * buy_fraction[t]
        fiat -= buy_quote_amount.sum()
        buy_base_amount = buy_quote_amount / order_price
        buy_sizes[t % buy_expiration] = buy_base_amount
        buy_prices[t % buy_expiration] = order_price
        # sells -> holds
        sell_base_amount = balance * sell_fraction[t]
        balance -= sell_base_amount
        sell_sizes[t % sell_expiration] = sell_base_amount
        sell_prices[t % sell_expiration] = order_price
        continue
    return fiat


def generate_price(t: int, mean: float, std: float) -> np.array:
    """
    Simulate log-normal price movements
    """
    moves = np.random.randn(t) * std + mean
    return np.exp(moves).cumprod()


def main() -> None:
    np.random.seed(42)
    m = 64
    t = 86400
    fee = 0.0008
    expiration = 5
    buy_fraction = (1 - (1 - np.random.rand(t, m)) ** (1 / 300)) / m
    sell_fraction = 1 - (1 - np.random.rand(t, m)) ** (1 / 300)
    price = np.hstack([np.expand_dims(generate_price(t, 0.00001, 0.0001), -1)
                       for _ in range(m)])
    drop = np.random.permutation(np.arange(price.size).reshape(*price.shape))
    price[drop % 11 == 0] = np.nan
    # numba warmup
    start = time.time()
    for i in range(7):
        print(simulate(100_000., buy_fraction, sell_fraction, price, fee,
                       expiration,
                       expiration))
    print(f"{time.time() - start:.2f}s")


if __name__ == '__main__':
    main()
