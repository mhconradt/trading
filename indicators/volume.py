from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient


class TrailingVolume:
    def __init__(self, periods: int):
        self.periods = periods

    @property
    def periods_required(self) -> int:
        return self.periods

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        candles = candles.unstack('market').tail(self.periods).stack('market')
        return candles.volume.groupby(level='market').sum()


class TrailingQuoteVolume:
    def __init__(self, periods: int):
        self.periods = periods

    @property
    def periods_required(self) -> int:
        return self.periods

    def compute(self, candles: pd.DataFrame) -> pd.Series:
        candles = candles.unstack('market').tail(self.periods).stack('market')
        return candles.quote_volume.groupby(level='market').sum()


class SplitQuoteVolume:
    def __init__(self, db: InfluxDBClient, periods: int, frequency: timedelta,
                 side: str):
        self.db = db
        self.side = side
        self.periods = periods
        self.frequency = frequency

    def compute(self) -> pd.DataFrame:
        query_api = self.db.query_api()
        query = """
            from(bucket: "trades")
              |> range(start: start)
              |> filter(fn: (r) => r["_measurement"] == "matches")
              |> filter(fn: (r) => r["side"] == side)
              |> filter(fn: (r) => r["_field"] == "price" or r["_field"] == "size")
              |> pivot(columnKey: ["_field"],
                       rowKey: ["_time", "market", "side"], 
                       valueColumn: "_value")
              |> map(fn: (r) => ({r with _value: r["price"] * r["size"]}))
              |> sum()
              |> pivot(columnKey: ["side"], 
                       rowKey: ["market"], 
                       valueColumn: "_value")
              |> yield(name: "mean")
        """
        params = {'start': -1 * self.periods * self.frequency,
                  'side': self.side}
        raw_df = query_api.query_data_frame(query, params=params,
                                            data_frame_index=['market'])
        if isinstance(raw_df, list):
            raw_df = pd.concat(raw_df)
        return raw_df[['market', '_value']].set_index('market')


def main():
    import time

    from settings import influx_db as influx_db_settings
    from indicators.sliding_candles import CandleSticks

    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    quote_volume = TrailingQuoteVolume(5)
    volume = TrailingVolume(5)
    total = 0.
    src = CandleSticks(influx_client, 'coinbasepro', 5,
                       timedelta(minutes=1))
    while True:
        start = time.time()
        candles = src.compute()
        values = quote_volume.compute(candles)
        print(values.isna().any())
        values = volume.compute(candles)
        print(values)
        total += time.time() - start


if __name__ == '__main__':
    main()
