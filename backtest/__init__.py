import numpy as np


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
    :param buy_expiration:
    :param sell_expiration:
    :return: the final market value of the portfolio
    """
    m = buy_fraction.shape[1]
    buy_sizes = np.zeros((buy_expiration, m))
    buy_prices = np.zeros((buy_expiration, m))
    sell_sizes = np.zeros((sell_expiration, m))
    sell_prices = np.zeros((sell_expiration, m))
    balance = np.zeros(m)
    for t in range(buy_fraction.shape[0]):
        # holds -> balance
        buy_fills = buy_prices > price[t]  # filled if market moves below price
        fiat_fill_total = (buy_sizes[buy_fills] * buy_prices[buy_fills]).sum()
        fiat -= fiat_fill_total * fee
        filled_size = np.where(buy_fills, buy_sizes, 0.).sum(axis=0)
        balance += filled_size
        buy_sizes[buy_fills] = 0.
        # holds -> fiat
        sell_fills = sell_prices < price[t]
        proceeds = (sell_sizes[sell_fills] * sell_prices[sell_fills]).sum()
        fiat += proceeds * (1 - fee)
        sell_sizes[sell_fills] = 0.
        # expiration
        balance += sell_sizes[t % sell_expiration]
        fiat += buy_sizes[t % buy_expiration] @ buy_prices[t % buy_expiration]
        # buys -> holds
        buy_quote_amount = fiat * buy_fraction[t]
        fiat -= buy_quote_amount.sum()
        buy_base_amount = buy_quote_amount / price[t]
        buy_sizes[t % buy_expiration] = buy_base_amount
        buy_prices[t % buy_expiration] = price[t]
        # sells -> holds
        sell_base_amount = balance * sell_fraction[t]
        balance -= sell_base_amount
        sell_sizes[t % sell_expiration] = sell_base_amount
        sell_prices[t % sell_expiration] = price[t]
        continue
    return fiat


def generate_price(t: int, mean: float, std: float) -> np.array:
    moves = np.random.randn(t) * std + mean
    return np.exp(moves).cumprod()


def main() -> None:
    np.random.seed(42)
    m = 3
    t = 86400
    fee = 0.0008
    buy_fraction = (1 - (1 - np.random.rand(t, m)) ** (1 / 300)) / m
    sell_fraction = 1 - (1 - np.random.rand(t, m)) ** (1 / 300)
    price = np.hstack([np.expand_dims(generate_price(t, 0.00001, 0.0001), -1)
                       for _ in range(3)])
    print(simulate(100_000., buy_fraction, sell_fraction, price, fee))


if __name__ == '__main__':
    main()
