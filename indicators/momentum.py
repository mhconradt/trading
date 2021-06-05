from datetime import timedelta

from influxdb_client import InfluxDBClient
from pandas import DataFrame, Series


class Momentum:
    def __init__(self, db: InfluxDBClient, exchange, frequency, start,
                 stop=timedelta(0)):
        self.db = db
        self.frequency = frequency
        self.exchange = exchange
        self.start = start
        self.stop = stop

    def compute(self) -> Series:
        query_api = self.db.query_api()
        parameters = {'exchange': self.exchange,
                      'freq': self.frequency,
                      'start': self.start - self.frequency,
                      'stop': self.stop,
                      'duration': 1 * self.frequency}
        df = query_api.query_data_frame("""
            at = from(bucket: "candles")
            |> range(start: start, stop: stop)
            |> filter(fn: (r) => r["_measurement"] == "candles_${string(v: freq)}")
            |> filter(fn: (r) => r["exchange"] == exchange)
            |> filter(fn: (r) => r["_field"] == "close")
            
            before = at |> timeShift(duration: duration)
            
            join(tables: {at: at, before: before}, 
                 on: ["market", "_time"], 
                 method: "inner")
            |> map(fn: (r) => ({_time: r["_time"], 
                market: r["market"], 
                momentum: (r["_value_at"] / r["_value_before"]) - 1.0
                }))
            |> yield()
        """, data_frame_index=['market', '_time'], params=parameters)
        return df.momentum


def main(influx: InfluxDBClient):
    _start = time.time()
    frequency = timedelta(minutes=1)
    mom = Momentum(influx, 'coinbasepro', frequency,
                   timedelta(hours=-1))
    values = mom.compute().unstack(0).resample(frequency).asfreq()
    # example of increasing momentum check
    increasing = values > values.shift(1)
    print(increasing.iloc[-1])


if __name__ == '__main__':
    from settings import influx_db as influx_db_settings
    import time

    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    main(influx_client)
