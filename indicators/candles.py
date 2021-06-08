from datetime import timedelta

from influxdb_client import InfluxDBClient
from pandas import DataFrame


class CandleSticks:
    def __init__(self, db: InfluxDBClient, exchange, frequency, start,
                 stop=timedelta(0)):
        self.db = db
        self.frequency = frequency
        self.exchange = exchange
        self.start = start
        self.stop = stop

    def compute(self) -> DataFrame:
        query_api = self.db.query_api()
        parameters = {'exchange': self.exchange,
                      'freq': self.frequency,
                      'start': self.start,
                      'stop': self.stop}
        # catchup mechanism for candlesticks?
        df = query_api.query_data_frame("""
            from(bucket: "candles")
            |> range(start: start, stop: stop)
            |> filter(fn: (r) => r["_measurement"] == "candles_${string(v: freq)}")
            |> filter(fn: (r) => r["exchange"] == exchange)
            |> pivot(rowKey: ["market", "_time"], columnKey: ["_field"], valueColumn: "_value")
            |> yield()
        """, data_frame_index=['market', '_time'],
                                        params=parameters)
        return df[['open', 'high', 'low', 'close', 'volume']]


def main(influx: InfluxDBClient):
    _start = time.time()
    sticks = CandleSticks(influx, 'coinbasepro', timedelta(minutes=15),
                          timedelta(hours=-3))
    print(sticks.compute())
    print(time.time() - _start)


if __name__ == '__main__':
    from settings import influx_db as influx_db_settings
    import time

    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    main(influx_client)
