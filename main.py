import pandas as pd

from trading.indicators import moon_score


def main() -> None:
    df = pd.read_parquet('.market_data')
    scores = moon_score(df)
    print(scores)


if __name__ == '__main__':
    main()
