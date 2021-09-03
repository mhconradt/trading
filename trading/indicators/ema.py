import logging
import time
from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient

from trading.helper.ttl_cache import ttl_cache

logger = logging.getLogger(__name__)


class TripleEMA:
    def __init__(self, db: InfluxDBClient, periods: int, frequency: timedelta,
                 quote: str):
        self.db = db
        self.periods = periods
        self.frequency = frequency
        self.quote = quote

    @ttl_cache(seconds=11.)
    def compute(self) -> pd.Series:
        _start = time.time()
        calculation_periods = 3 * self.periods - 2
        start = -calculation_periods * self.frequency
        params = {'periods': self.periods, 'start': start,
                  'frequency': self.frequency,
                  'quote': self.quote}
        query = """
            measurement = "candles_" + string(v: frequency)
        
            from(bucket: "candles")
              |> range(start: start)
              |> filter(fn: (r) => r["_measurement"] == measurement)
              |> filter(fn: (r) => r["quote"] == quote)
              |> filter(fn: (r) => r["_field"] == "close")
              |> tail(n: 3 * periods - 2)
              |> tripleEMA(n: periods)
              |> yield(name: "ema")
        """
        index = ['market', '_time']
        query_api = self.db.query_api()
        raw_df = query_api.query_data_frame(query, params=params,
                                            data_frame_index=index)
        if not len(raw_df):
            raise Exception()
        if isinstance(raw_df, list):
            raw_df = pd.concat(raw_df)
        df = raw_df['_value'].unstack('market')
        logger.debug(f"Query took {time.time() - _start:.2f}s")
        return df.iloc[-1]  # convert to series


def main():
    import time
    from datetime import timedelta

    from trading.settings import influx_db as influx_db_settings
    _influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                             influx_db_settings.INFLUX_TOKEN,
                             org_id=influx_db_settings.INFLUX_ORG_ID,
                             org=influx_db_settings.INFLUX_ORG)
    ema = TripleEMA(_influx, 26, timedelta(minutes=1), 'USD')
    while True:
        try:
            _start = time.time()
            _values = ema.compute()
            print(f"Took {time.time() - _start:.2f}s")
        except (Exception,):
            pass


if __name__ == '__main__':
    main()
