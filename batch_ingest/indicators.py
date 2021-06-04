import pandas as pd
import matplotlib.pyplot as plt


def _leading_momentum(df: pd.DataFrame, lead: int, span: int) -> pd.Series:
    return (df.close.shift(lead) / df.close.shift(lead + span)) - 1.


def leading_momentum(df: pd.DataFrame, lead: int = 1,
                     span: int = 1) -> pd.Series:
    return df.groupby(level=['freq', 'market'], group_keys=False).apply(
        _leading_momentum,
        lead=lead,
        span=span)


def _momentum(df: pd.DataFrame, span: int) -> pd.Series:
    return (df.close / df.close.shift(span)) - 1.


def momentum(df: pd.DataFrame, span: int = 1) -> pd.Series:
    return df.groupby(level=['freq', 'market'], group_keys=False).apply(
        _momentum, span=span)


def autocorrelation(df: pd.DataFrame, lag: int = 1):
    return df.open.groupby(level=['freq', 'market'], group_keys=False).apply(
        lambda s: s.autocorr(lag))


def moon_score(df: pd.DataFrame) -> pd.Series:
    short_term = df.loc[['15m', '5m']]
    mom = momentum(short_term)
    mom1 = leading_momentum(short_term)
    mom_15m, mom1_15m = mom.loc['15m'], mom1.loc['15m']
    moon_15m = (mom_15m > mom1_15m) * (mom_15m - mom_15m)
    mom_5m, mom1_5m = mom.loc['5m'], mom1.loc['5m']
    moon_5m = (mom_5m > mom1_5m) * (mom_5m - mom1_5m)
    # candle for t-15min is most recent at t
    adj = moon_15m.unstack(0).shift(1).resample('5min').ffill(2)
    return moon_5m.unstack(0) + adj


def usd_mask(df: pd.DataFrame) -> pd.DataFrame:
    cols = df.columns
    is_usd = cols.str.endswith('-USD')
    return df[cols[is_usd]]


def main() -> None:
    df = pd.read_parquet('../.market_data')
    scores = moon_score(df)
    scores[['ADA-USD', 'BTC-USD', 'ETH-USD', 'LTC-USD']].plot()
    plt.show()


__all__ = ['momentum', 'leading_momentum', 'moon_score']


if __name__ == '__main__':
    main()
