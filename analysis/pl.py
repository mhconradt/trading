"""
Perform P/L analysis on fills report.
"""
import typing as t
from decimal import Decimal

import pandas as pd
from pandas import DataFrame


def is_buy(trade: dict) -> bool:
    return trade['side'] == 'BUY'


def is_sell(trade: dict) -> bool:
    return trade['side'] == 'SELL'


def trade_profitability(trades: t.List[dict]) -> None:
    buy_gross_size, buy_gross_quote_size = Decimal(0), Decimal(0)
    buy_net_size, buy_net_quote_size = Decimal(0), Decimal(0)
    buy_i, buy_j = 0, 0
    sell_gross_size, sell_gross_quote_size = Decimal(0), Decimal(0)
    sell_net_size, sell_net_quote_size = Decimal(0), Decimal(0)
    sell_i, sell_j = 0, 0
    n = len(trades)
    for i in range(n):
        trade_i = trades[i]
        if is_buy(trade_i):
            # advance right side of sell window
            gross_net_margin = sell_gross_size - sell_net_size
            sell_net_size += trade_i['size']
            if gross_net_margin >= 0:
                marginal_price = trades[sell_j]['price']
                if trade_i['size'] >= gross_net_margin:
                    sell_net_quote_size += gross_net_margin * marginal_price
                    # ADVANCE WINDOW ONE STEP
                    for j in range(sell_j + 1, n):
                        if is_sell(trades[j]):
                            sell_j = j
                            break
                else:
                    sell_net_quote_size += trade_i['size'] * marginal_price
            for j in range(sell_j, n):
                net_gross_margin = sell_net_size - sell_gross_size
                if net_gross_margin <= 0:
                    break
                trade_j = trades[j]
                if is_sell(trade_j):
                    sell_j = j
                    sell_gross_size += trade_j['size']
                    sell_gross_quote_size += trade_j['size'] * trade_j['price']
                    delta_net_size = min(net_gross_margin, trade_j['size'])
                    sell_net_quote_size += delta_net_size * trade_j['price']
                else:
                    continue
            # annotate w/ average sell price
            trade_i['avg_sell_price'] = sell_gross_quote_size / sell_gross_size
            # advance right side of buy window
            buy_j = i
            buy_gross_size += trade_i['size']
            buy_net_size += trade_i['size']
            buy_gross_quote_size += trade_i['size'] * trade_i['price']
            buy_net_quote_size += trade_i['size'] * trade_i['price']
        else:
            # advance left side of sell window
            sell_gross_size -= trade_i['size']
            sell_net_size -= trade_i['size']
            sell_gross_quote_size -= trade_i['size'] * trade_i['price']
            sell_net_quote_size -= trade_i['size'] * trade_i['price']
            for j in range(i + 1, n):
                if is_sell(trades[j]):
                    sell_i = j
                    break
            # annotate with average buy price
            trade_i['avg_buy_price'] = buy_gross_quote_size / buy_gross_size
            # advance left side of buy window
            gross_net_margin = buy_gross_size - buy_net_size
            buy_net_size -= trade_i['size']
            # take a little bit off each trade
            if gross_net_margin:
                marginal_price = trades[buy_i]['price']
                remaining = trades[buy_i]['size'] - gross_net_margin
                if trade_i['size'] >= remaining:
                    # REMOVE BUY_I FROM WINDOW VARS
                    buy_net_quote_size -= remaining * marginal_price
                    buy_gross_size -= trades[buy_i]['size']
                    buy_gross_quote_size -= trades[buy_i][
                                                'size'] * marginal_price
                    # ADVANCE LEFT SIDE OF BUY WINDOW ONE STEP
                    for j in range(buy_i + 1, n):
                        if is_buy(trades[j]):
                            buy_i = j
                            break
                else:
                    buy_net_quote_size -= trade_i['size'] * marginal_price
            for j in range(buy_i, buy_j + 1):
                trade_j = trades[j]
                if is_buy(trade_j):
                    buy_i = j
                    margin = buy_gross_size - buy_net_size
                    delta_net_size = min(trade_j['size'], margin)
                    buy_net_quote_size -= delta_net_size * trade_j['price']
                    if margin < trade_j['size']:
                        break
                    buy_gross_size -= trade_j['size']
                    buy_gross_quote_size -= trade_j['size'] * trade_j['price']
                else:
                    continue


def zero_pad(fills: DataFrame, account: DataFrame) -> DataFrame:
    roots = account[account.balance == 0]
    start = roots.groupby('amount/balance unit').time.min()
    end = roots.groupby('amount/balance unit').time.max()
    start, end = start[start != end], end[start != end]
    fills = fills.set_index(['size unit', 'created at'])
    products = list(start.index)
    return pd.concat([fills.loc[p].loc[start[p]:end[p]] for p in products],
                     keys=products, names=['product', 'time'])


def transform(fills: DataFrame) -> DataFrame:
    return fills.assign(price=fills.price.map(Decimal),
                        size=fills['size'].map(Decimal))


if __name__ == '__main__':
    _fills = pd.read_csv('/Users/maxwellconradt/Downloads/fills.csv',
                         parse_dates=['created at'])
    _account = pd.read_csv('/Users/maxwellconradt/Downloads/account.csv',
                           parse_dates=['time'])
    zpd = zero_pad(_fills[_fills.portfolio == 'Point42 Blue'],
                   _account[_account.portfolio == 'Point42 Blue'])
    sol_trades = transform(zpd.loc['SOL']).to_dict(orient='records')
    trade_profitability(sol_trades)
    df = DataFrame(sol_trades)
    print(df)
