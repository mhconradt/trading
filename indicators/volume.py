import logging
import time
from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient

logger = logging.getLogger(__name__)


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
    def __init__(self, db: InfluxDBClient, periods: int, frequency: timedelta):
        self.db = db
        self.periods = periods
        self.frequency = frequency

    def compute(self) -> pd.DataFrame:
        _start = time.time()
        query_api = self.db.query_api()
        query = """
            from(bucket: "trades")
              |> range(start: start)
              |> filter(fn: (r) => r["_measurement"] == "matches")
              |> filter(fn: (r) => r["_field"] == "price" or r["_field"] == "size")
              |> pivot(columnKey: ["_field"],
                       rowKey: ["_time", "market", "side"], 
                       valueColumn: "_value")
              |> map(fn: (r) => ({r with _value: r["price"] * r["size"]}))
              |> sum()
              |> pivot(columnKey: ["side"], rowKey: ["market"], valueColumn: "_value")
              |> yield(name: "split")
        """
        params = {'start': -1 * self.periods * self.frequency}
        raw_df = query_api.query_data_frame(query, params=params,
                                            data_frame_index=['market'])
        if isinstance(raw_df, list):
            raw_df = pd.concat(raw_df)
        logger.debug(f"Query took {time.time() - _start:.2f}s")
        return raw_df[['buy', 'sell']].fillna(0.)


def main():
    import time

    from settings import influx_db as influx_db_settings
    from indicators.sliding_candles import CandleSticks

    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    src = CandleSticks(influx_client, 'coinbasepro', 5,
                       frequency=timedelta(minutes=1), offset=0)
    qv = TrailingQuoteVolume(5)
    splits = SplitQuoteVolume(influx_client, 5, timedelta(minutes=1))
    total = 0.
    while True:
        try:
            candles = src.compute()
            qv_values = qv.compute(candles)
            percentages = qv_values / qv_values.sum()
            print(percentages.sort_values(ascending=False).head(10))
            print(percentages.sort_values().head(10))
            start = time.time()
            # values = splits.compute()
            # print(values)
            total += time.time() - start
        except (Exception,) as e:
            print(e)
            pass


if __name__ == '__main__':
    main()
