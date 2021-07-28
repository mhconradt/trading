from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient

from exceptions import StaleDataException


class CandleSticks:
    def __init__(self, db: InfluxDBClient, exchange: str, periods: int,
                 frequency: timedelta, offset: int):
        self.db = db
        self.frequency = frequency
        self.exchange = exchange
        self.periods = periods
        self.offset = offset

    def compute(self) -> pd.DataFrame:
        query_api = self.db.query_api()
        lag_toleration = timedelta(seconds=15)
        start = -(
                    self.periods + 1 + self.offset) * self.frequency + lag_toleration
        stop = self.frequency * -self.offset
        parameters = {'exchange': self.exchange,
                      'freq': self.frequency,
                      'start': start,
                      'stop': stop}
        df = query_api.query_data_frame("""
            from(bucket: "candles")
            |> range(start: start, stop: stop)
            |> filter(fn: (r) => r["_measurement"] == "candles_${string(v: freq)}")
            |> filter(fn: (r) => r["exchange"] == exchange)
            |> pivot(rowKey: ["market", "_time"], 
                     columnKey: ["_field"], 
                     valueColumn: "_value")
            |> yield()
        """, data_frame_index=['market', '_time'],
                                        params=parameters)
        if not len(df):
            raise StaleDataException(
                f"No candles between {start} and {stop}"
            )
        if isinstance(df, list):
            df = pd.concat(df)
        metrics = ['open', 'high', 'low', 'close', 'volume', 'quote_volume']
        candles = df[metrics]
        # only show data for the last n times
        return candles.unstack('market').tail(self.periods).stack('market')


def main(influx: InfluxDBClient):
    import time
    candles = CandleSticks(influx, 'coinbasepro', 300, timedelta(minutes=1), 0)
    total = 0.
    measurements = 7
    for i in range(measurements):
        start = time.time()
        values = candles.compute()
        print(values)
        total += time.time() - start
    print(total / measurements)


if __name__ == '__main__':
    from settings import influx_db as influx_db_settings

    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    main(influx_client)
