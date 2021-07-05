from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient

from exceptions import StaleDataException
from indicators.ticker import Ticker


class Momentum:
    def __init__(self, db: InfluxDBClient, exchange: str, frequency: timedelta,
                 start: timedelta, stop=timedelta(0)):
        self.db = db
        self.frequency = frequency
        self.exchange = exchange
        self.start = start
        self.stop = stop

    def compute(self) -> pd.DataFrame:
        query_api = self.db.query_api()
        parameters = {'exchange': self.exchange,
                      'freq': self.frequency,
                      'start': self.start - self.frequency,
                      'stop': self.stop,
                      'duration': self.frequency}
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
        if not len(df):
            raise StaleDataException(
                f"No momentum between {self.start} and {self.stop}"
            )
        return df.momentum.unstack(0)


class IncrementalMomentum:
    def __init__(self, db: InfluxDBClient, exchange: str, frequency: timedelta,
                 start: timedelta, stop=timedelta(0), span: int = 1):
        self.db = db
        self.frequency = frequency
        self.exchange = exchange
        self.start = start
        self.stop = stop
        self.ticker = Ticker(db, exchange)
        self.span = span

    def compute(self) -> pd.DataFrame:
        query_api = self.db.query_api()
        parameters = {'exchange': self.exchange,
                      'freq': self.frequency,
                      'start': self.start - self.span * self.frequency - timedelta(
                          seconds=15),
                      'stop': self.stop}
        df = query_api.query_data_frame("""
            from(bucket: "candles")
            |> range(start: start, stop: stop)
            |> filter(fn: (r) => r["_measurement"] == "candles_${string(v: freq)}")
            |> filter(fn: (r) => r["exchange"] == exchange)
            |> filter(fn: (r) => r["_field"] == "close")
            |> yield()
        """, data_frame_index=['market', '_time'], params=parameters)
        if not len(df):
            raise StaleDataException(
                f"No momentum between {self.start} and {self.stop}"
            )
        close = df['_value'].unstack(0)
        ticker = self.ticker.compute()
        closes = close.append(ticker)
        return (closes / closes.shift(self.span)).iloc[self.span:] - 1.


def main(influx: InfluxDBClient):
    mom = IncrementalMomentum(influx, 'coinbasepro',
                              frequency=timedelta(minutes=5),
                              start=timedelta(minutes=-30) - timedelta(seconds=15),
                              stop=timedelta(0))
    print(mom.compute()['NKN-USD'])


if __name__ == '__main__':
    from settings import influx_db as influx_db_settings

    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    main(influx_client)
