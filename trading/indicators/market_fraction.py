import time
from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient

from trading.helper.ttl_cache import ttl_cache


class MarketFraction:
    def __init__(self, db: InfluxDBClient, periods: int, frequency: timedelta,
                 quote: str):
        self.db = db
        self.periods = periods
        self.frequency = frequency
        self.quote = quote

    @ttl_cache(seconds=31.)
    def compute(self) -> pd.Series:
        params = {'start': -self.periods * self.frequency,
                  'freq': self.frequency,
                  'quote': self.quote}
        query = """
            measurement = "candles_" + string(v: freq)
        
            from(bucket: "candles")
                |> range(start: start)
                |> filter(fn: (r) => r["_measurement"] == measurement)
                |> filter(fn: (r) => r["quote"] == quote)
                |> filter(fn: (r) => r["_field"] == "quote_volume")
                |> sum()
                |> yield(name: "quote_volume")
        """
        df = self.db.query_api().query_data_frame(query,
                                                  params=params,
                                                  data_frame_index=['market'])
        return df['_value'] / df['_value'].sum()


def main(influx: InfluxDBClient) -> None:
    indicator = MarketFraction(influx, periods=60,
                               frequency=timedelta(minutes=1),
                               quote='USD')
    for _ in range(7):
        _start = time.time()
        ranges = indicator.compute()
        print(ranges.sort_values(ascending=False).head())
        print(f"{time.time() - _start:.2f}")
        time.sleep(15)


if __name__ == '__main__':
    from trading.settings import influx_db as influx_db_settings

    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    main(influx_client)
