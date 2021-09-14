import numpy as np


def simulate(fiat: float, buy_fraction: np.array, sell_fraction: np.array,
             price: np.array, fee: float) -> np.array:
    """
    Simulate a portfolio's trading activity + returns
    :param fiat: the starting fiat balance
    :param buy_fraction: [t, m] = fraction of fiat to spend on m at t
    :param sell_fraction: [t, m] = fraction of balance[m] to sell at t
    :param price: [t, m] = price of m at t
    :param fee: fee paid to exchange as fraction of spending amount
    :return: the final market value of the portfolio
    """
    m = buy_fraction.shape[1]
    balance = np.zeros(m)
    for t in range(buy_fraction.shape[0]):
        buy_quote_amount = fiat * buy_fraction[t]
        buy_quote_total = buy_quote_amount.sum()
        fiat -= buy_quote_total * (1 + fee)
        buy_base_amount = buy_quote_amount / price[t]
        balance += buy_base_amount
        sell_base_amount = balance * sell_fraction[t]
        balance -= sell_base_amount
        sell_quote_amount = sell_base_amount * price[t]
        sell_quote_total = sell_quote_amount.sum()
        fiat += sell_quote_total * (1 - fee)
    return fiat
