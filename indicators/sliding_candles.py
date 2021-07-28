from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient

from exceptions import StaleDataException


class CandleSticks:
    def __init__(self, db: InfluxDBClient, exchange: str, periods: int,
                 frequency: timedelta, offset=0):
        self.db = db
        self.frequency = frequency
        self.exchange = exchange
        self.periods = periods
        self.offset = offset

    def compute(self) -> pd.DataFrame:
        query_api = self.db.query_api()
        start = -(self.periods + self.offset) * self.frequency
        stop = -self.offset * self.frequency
        parameters = {'exchange': self.exchange,
                      'freq': self.frequency,
                      'start': start,
                      'stop': stop}
        raw_df = query_api.query_data_frame("""
            import "date"
            
            offset = duration(v: int(v: now()) - int(v: date.truncate(t: now(),
                                                     unit: freq)))    

            trades = from(bucket: "trades")
                |> range(start: start, stop: stop)
                |> filter(fn: (r) => r["_measurement"] == "matches")
                |> filter(fn: (r) => r["_field"] == "price" 
                                     or r["_field"] == "size")
                |> filter(fn: (r) => r["exchange"] == exchange)                
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
        return candles


def main():
    import time

    from settings import influx_db as influx_db_settings

    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    candles = CandleSticks(influx_client, 'coinbasepro', 30,
                           timedelta(minutes=1))
    while True:
        start = time.time()
        values = candles.compute()
        print(values)
        print(f"Took {time.time() - start:.2f}s")


if __name__ == '__main__':
    main()
