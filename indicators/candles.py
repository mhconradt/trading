from datetime import timedelta

from influxdb_client import InfluxDBClient
from pandas import DataFrame


class CandleSticks:
    def __init__(self, db: InfluxDBClient, start, frequency):
        self.db = db
        self.start = start
        self.frequency = frequency

    def compute(self) -> DataFrame:
        query_api = self.db.query_api()
        parameters = {'_start': self.start - self.frequency,
                      '_every': self.frequency}
        df = query_api.query_data_frame("""
            import "date"

            high = from(bucket: "trading")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "matches")
              |> filter(fn: (r) => r["_field"] == "price")
              |> aggregateWindow(every: _every, fn: max, timeSrc: "_start")
              |> keep(columns: ["_time", "market", "_value"])
              |> rename(columns: {_value: "high"})
            
            low = from(bucket: "trading")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "matches")
              |> filter(fn: (r) => r["_field"] == "price")
              |> aggregateWindow(every: _every, fn: min, timeSrc: "_start")
              |> keep(columns: ["_time", "market", "_value"])
              |> rename(columns: {_value: "low"})
            
            open = from(bucket: "trading")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "matches")
              |> filter(fn: (r) => r["_field"] == "price")
              |> aggregateWindow(every: _every, fn: first, timeSrc: "_start")
              |> keep(columns: ["_time", "market", "_value"])
              |> rename(columns: {_value: "open"})
            
            close = from(bucket: "trading")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "matches")
              |> filter(fn: (r) => r["_field"] == "price")
              |> aggregateWindow(every: _every, fn: last, timeSrc: "_start")
              |> keep(columns: ["_time", "market", "_value"])
              |> rename(columns: {_value: "close"})
            
            hl = join(tables: {high: high, low: low}, on: ["_time", "market"], method: "inner")
            
            ohl = join(tables: {hl: hl, open: open}, on: ["_time", "market"], method: "inner")
            
            
            ohlc = join(tables: {ohl: ohl, close: close}, 
                        on: ["_time", "market"],
                        method: "inner") 
                        |> filter(fn: (r) => date.nanosecond(t: r["_time"]) == 0)
                        |> yield()
        """, data_frame_index=['market', '_time'], params=parameters)
        return df[['open', 'high', 'low', 'close']]


if __name__ == '__main__':
    from settings import influx_db as influx_db_settings
    import time

    _influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                             influx_db_settings.INFLUX_TOKEN,
                             org=influx_db_settings.INFLUX_ORG)
    _start = time.time()
    print(CandleSticks(_influx, timedelta(hours=-1),
                       timedelta(minutes=15)).compute())
    print(time.time() - _start)
    _start = time.time()
    print(CandleSticks(_influx, timedelta(hours=-1),
                       timedelta(minutes=5)).compute())
    print(time.time() - _start)
