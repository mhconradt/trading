from datetime import timedelta

from influxdb_client import InfluxDBClient
from pandas import Series


def momentum(close: Series, span: int = 1, lead: int = 0):
    return (close.shift(lead) / close.shift(lead + span)) - 1.


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
                      'stop': self.stop}
        df = query_api.query_data_frame("""
            from(bucket: "candles")
            |> range(start: start, stop: stop)
            |> filter(fn: (r) => r["_measurement"] == "candles_${string(v: freq)}")
            |> filter(fn: (r) => r["exchange"] == exchange)
            |> filter(fn: (r) => r["_field"] == "close")
            |> yield(name: "close")
        """, data_frame_index=['market', '_time'],
                                        params=parameters)
        close = df['_value'].rename('close')
        return close.groupby(level=0).apply(momentum)


def main(influx: InfluxDBClient):
    _start = time.time()
    mom = Momentum(influx, 'coinbasepro', timedelta(minutes=15),
                   timedelta(hours=-1))
    print(mom.compute())
    print(time.time() - _start)


if __name__ == '__main__':
    from settings import influx_db as influx_db_settings
    import time

    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    main(influx_client)
