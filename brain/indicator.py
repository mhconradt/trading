from abc import ABC

from influxdb_client import InfluxDBClient
from pandas import DataFrame, Series


class MostRecentPrice:
    def __init__(self, db: InfluxDBClient):
        self.db = db

    def compute(self) -> Series:
        query_api = self.db.query_api()
        df = query_api.query_data_frame("""
            from(bucket: "trading")
                |> range(start: -1m)
                |> filter(fn: (r) => r["_measurement"] == "matches")
                |> filter(fn: (r) => r["_field"] == "price")
                |> last()
                |> yield(name: "price")
        """, data_frame_index=['market'])
        aliases = {'_value': 'price', '_time': 'timestamp'}
        return df[['_value', '_time']].rename(aliases, axis=1)


if __name__ == '__main__':
    import time

    from settings import influx_db as influx_db_settings

    _influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                             influx_db_settings.INFLUX_TOKEN,
                             org=influx_db_settings.INFLUX_ORG)

    most_recent = MostRecentPrice(_influx)
    for _ in range(12):
        prices = most_recent.compute()
        print(prices.timestamp.max())
        time.sleep(5)
