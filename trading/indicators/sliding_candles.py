import logging
import time
from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient

from trading.exceptions import StaleDataException

logger = logging.getLogger(__name__)


class CandleSticks:
    def __init__(self, db: InfluxDBClient, periods: int, frequency: timedelta,
                 bucket: str, quote: str):
        self.bucket = bucket
        self.db = db
        self.frequency = frequency
        self.periods = periods
        self.quote = quote

    def compute(self) -> pd.DataFrame:
        _start = time.time()
        query_api = self.db.query_api()
        start = -self.periods * self.frequency
        parameters = {'freq': self.frequency,
                      'start': start,
                      'bucket': self.bucket,
                      'quote': self.quote}
        raw_df = query_api.query_data_frame("""
            import "date"
            
            offset = duration(v: int(v: now()) - int(v: date.truncate(t: now(),
                                                     unit: freq)))    

            trades = from(bucket: bucket)
                |> range(start: start)
                |> filter(fn: (r) => r["_measurement"] == "matches")
                |> filter(fn: (r) => r["quote"] == quote)
                |> filter(fn: (r) => r["_field"] == "price" 
                                     or r["_field"] == "size")
                |> keep(columns: ["_time", "market", "_value", "_field"])
                |> window(every: freq, period: freq, offset: offset)
            
            prices = trades
              |> filter(fn: (r) => r["_field"] == "price")
        
            high = prices
              |> max()
              |> yield(name: "high")

            low = prices
              |> min()
              |> yield(name: "low")

            open = prices
              |> first()
              |> yield(name: "open")

            close = prices
              |> last()
              |> yield(name: "close")

            volume = trades
              |> filter(fn: (r) => r["_field"] == "size")
              |> sum()
              |> yield(name: "volume")

            quote_volume = trades
              |> pivot(rowKey: ["_time", "market"],
                       columnKey: ["_field"],
                       valueColumn: "_value")
              |> map(fn: (r) => ({ r with _value: r["price"] * r["size"]}))
              |> sum()
              |> yield(name: "quote_volume")
        """, data_frame_index=['market', '_start', 'result'],
                                            params=parameters)
        if not len(raw_df):
            raise StaleDataException(f"No candles after {start}")
        if isinstance(raw_df, list):
            raw_df = pd.concat(raw_df)
        candles = raw_df['_value'].unstack('result')
        times = candles.index.levels[candles.index.names.index('_start')]
        if times.nunique() < self.periods:
            raise StaleDataException("Insufficient data.")
        logger.debug(f"Query took {time.time() - _start:.2f}s")
        return candles


def main():
    import time

    from trading.settings import influx_db as influx_db_settings

    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    candles = CandleSticks(influx_client, 30, timedelta(minutes=1), 'level1',
                           'USD')
    while True:
        start = time.time()
        values = candles.compute()
        print(values)
        print(f"Took {time.time() - start:.2f}s")


if __name__ == '__main__':
    main()
