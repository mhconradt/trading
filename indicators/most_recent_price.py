from datetime import datetime

from influxdb_client import InfluxDBClient
from pandas import DataFrame


class MostRecentPrice:
    def __init__(self, db: InfluxDBClient, exchange: str = 'coinbasepro'):
        self.exchange = exchange
        self.db = db

    def compute(self) -> DataFrame:
        query_api = self.db.query_api()
        parameters = {'_exchange': self.exchange}
        df = query_api.query_data_frame("""
            from(bucket: "trades")
                |> range(start: -1m)
                |> filter(fn: (r) => r["_measurement"] == "matches")
                |> filter(fn: (r) => r["exchange"] == _exchange)
                |> filter(fn: (r) => r["_field"] == "price")
                |> last()
                |> yield(name: "price")
        """, data_frame_index=['market'], params=parameters)
        aliases = {'_value': 'price', '_time': 'timestamp'}
        return df[['_value', '_time']].rename(aliases, axis=1)


if __name__ == '__main__':
    import time

    from settings import influx_db as influx_db_settings

    _influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                             influx_db_settings.INFLUX_TOKEN,
                             org_id=influx_db_settings.INFLUX_ORG_ID,
                             org=influx_db_settings.INFLUX_ORG)

    most_recent = MostRecentPrice(_influx)
    while True:
        prices = most_recent.compute()
        print(prices)
        time.sleep(5)
