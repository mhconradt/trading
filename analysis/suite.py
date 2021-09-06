"""
Perform analysis on fills report.
"""

import pandas as pd
from matplotlib import pyplot as plt
from pandas import DataFrame

from analysis.pl import analyze as analyze_pl


def plot_pl(pl: DataFrame) -> None:
    abs_pl_swings = pl.abs().groupby(level='product').sum()
    top_markets = abs_pl_swings.sort_values().tail(10).index
    pl = pl[top_markets].unstack('product')
    hourly_pl = pl.resample('1h').sum()
    hourly_pl.plot()
    plt.show()


def main():
    fills = pd.read_csv('/Users/maxwellconradt/Downloads/fills.csv',
                        parse_dates=['created at'])
    account = pd.read_csv('/Users/maxwellconradt/Downloads/account.csv',
                          parse_dates=['time'])
    fills = fills[fills.portfolio == 'Point42 Blue']
    account = account[account.portfolio == 'Point42 Blue']
    df = analyze_pl(account, fills)
    plot_pl(df)
    print(df)


if __name__ == '__main__':
    main()
