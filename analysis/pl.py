import typing as t
from decimal import Decimal

import pandas as pd
from pandas import DataFrame


def is_buy(trade: dict) -> bool:
    return trade['side'] == 'BUY'


def is_sell(trade: dict) -> bool:
    return trade['side'] == 'SELL'


def fifo_pl(trades: t.List[dict]) -> None:
    j = 0
    sell_size = Decimal(0)
    sell_price = Decimal(0)
    n = len(trades)
    for i in range(n):
        trade_i = trades[i]
        if is_buy(trade_i):
            buy_size = trade_i['size']
            size = Decimal(0)
            quote_size = Decimal(0)
            for k in range(j, n):
                if not buy_size:
                    break
                if is_sell(trades[k]):
                    if not sell_size:
                        sell_size = trades[k]['size']
                        sell_price = trades[k]['price']
                        j = k
                    delta_size = min(sell_size, buy_size)
                    buy_size -= delta_size
                    sell_size -= delta_size
                    size += delta_size
                    quote_size += delta_size * sell_price
                else:
                    continue
            trade_i['avg_sell_price'] = quote_size / size
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
                     keys=products,
                     names=['product', 'time'])


def transform_in(fills: DataFrame) -> t.List[dict]:
    orig = fills.assign(price=fills.price.map(Decimal),
                        size=fills['size'].map(Decimal))
    return orig.drop('product', axis=1).reset_index().to_dict(orient='records')


def get_minutely_pl(df: pd.DataFrame) -> pd.DataFrame:
    buys = df[df.side == 'BUY']
    pl = (buys.avg_sell_price - buys.price) * buys['size']
    grouper = pl.groupby(level='product')
    tpl = grouper.apply(lambda s: s.droplevel('product').resample('T').sum())
    return tpl


def transform_out(trades: t.List[dict]) -> pd.DataFrame:
    df = pd.DataFrame(trades)
    df = df.assign(avg_sell_price=pd.to_numeric(df.avg_sell_price),
                   price=pd.to_numeric(df.price),
                   size=pd.to_numeric(df['size'])
                   ).reset_index().set_index(['product', 'time'])
    return df


def analyze(account: DataFrame, fills: DataFrame):
    zpd = zero_pad(fills, account)
    trades = transform_in(zpd)
    i = 0
    product = None
    for j in range(len(trades)):
        if trades[j]['product'] != product:
            product = trades[j]['product']
            fifo_pl(trades[i:j])
            i = j
    trade_pl = transform_out(trades)
    return get_minutely_pl(trade_pl)
