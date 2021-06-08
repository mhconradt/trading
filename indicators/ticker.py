from datetime import timedelta

from influxdb_client import InfluxDBClient
from pandas import DataFrame


class Ticker:
    def __init__(self, db: InfluxDBClient, exchange: str = 'coinbasepro',
                 start: timedelta = timedelta(minutes=-1),
                 stop: timedelta = timedelta(0)):
        self.exchange = exchange
        self.db = db
        self.start = start
        self.stop = stop

    def compute(self) -> DataFrame:
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
        aliases = {'_value': 'price', '_time': 'timestamp'}
        return df[['_value', '_time']].rename(aliases, axis=1)


if __name__ == '__main__':
    import time

    from settings import influx_db as influx_db_settings

    _influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                             influx_db_settings.INFLUX_TOKEN,
                             org_id=influx_db_settings.INFLUX_ORG_ID,
                             org=influx_db_settings.INFLUX_ORG)

    ticker = Ticker(_influx, start=timedelta(days=-1))
    while True:
        tickers = ticker.compute()
        print(tickers)
        print(tickers[tickers.index.str.endswith('-USD')])
        time.sleep(5)
