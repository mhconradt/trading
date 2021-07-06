from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient

from exceptions import StaleDataException


class Ticker:
    def __init__(self, db: InfluxDBClient, exchange: str,
                 start: timedelta = timedelta(minutes=-1),
                 stop: timedelta = timedelta(0)):
        self.exchange = exchange
        self.db = db
        self.start = start
        self.stop = stop

    def compute(self) -> pd.Series:
        query_api = self.db.query_api()
        parameters = {'exchange': self.exchange,
                      'start': self.start,
                      'stop': self.stop}
        df = query_api.query_data_frame("""
            from(bucket: "trades")
                |> range(start: start, stop: stop)
                |> filter(fn: (r) => r["_measurement"] == "matches")
                |> filter(fn: (r) => r["exchange"] == exchange)
                |> filter(fn: (r) => r["_field"] == "price")
                |> keep(columns: ["_time", "_value", "market"])
                |> last()
                |> yield(name: "price")
        """, data_frame_index=['market'], params=parameters)
        if not len(df):
            raise StaleDataException(
                f"No prices between {self.start} and {self.stop}"
            )
        return df['_value'].rename('price')


if __name__ == '__main__':
    import time

    from settings import influx_db as influx_db_settings

    _influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                             influx_db_settings.INFLUX_TOKEN,
                             org_id=influx_db_settings.INFLUX_ORG_ID,
                             org=influx_db_settings.INFLUX_ORG)

    ticker = Ticker(_influx, 'coinbasepro', start=timedelta(minutes=-5))
    total = 0.
    samples = 7
    for i in range(samples):
        _start = time.time()
        values = ticker.compute()
        print(values[values.index.str.endswith('-USD')])
        total += time.time() - _start
    print(total / samples)
