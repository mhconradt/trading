import logging
import time
from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient

logger = logging.getLogger(__name__)


class BidAsk:
    def __init__(self, db: InfluxDBClient, period: timedelta, bucket: str):
        self.bucket = bucket
        self.db = db
        self.period = period

    def compute(self) -> pd.DataFrame:
        """
        :return: the most recent bid and ask prices
        """
        _start = time.time()
        query_api = self.db.query_api()
        params = {'start': -self.period, 'bucket': self.bucket}
        query = """
            from(bucket: bucket)
                |> range(start: start)
                |> filter(fn: (r) => r["_measurement"] == "tickers")
                |> last()
                |> pivot(rowKey: ["market"], 
                         columnKey: ["_field"], 
                         valueColumn: "_value")
                |> yield(name: "bid_ask")
        """
        raw_df = query_api.query_data_frame(query,
                                            params=params,
                                            data_frame_index=['market'])
        if isinstance(raw_df, list):
            raw_df = pd.concat(raw_df)
        df = raw_df[['bid', 'ask']]
        logger.debug(f"Query took {time.time() - _start:.2f}s")
        return df


def main():
    import time
    from datetime import timedelta
    from settings import influx_db as influx_db_settings
    _influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                             influx_db_settings.INFLUX_TOKEN,
                             org_id=influx_db_settings.INFLUX_ORG_ID,
                             org=influx_db_settings.INFLUX_ORG)
    bid_ask = BidAsk(_influx, timedelta(minutes=1), 'tickers')
    while True:
        try:
            _start = time.time()
            df = bid_ask.compute()
            spread = df['ask'] - df['bid']
            print(spread['ETH-USD'])
            print(f"Took {time.time() - _start:.2f}s")
        except (Exception,) as e:
            print(e)
            pass


if __name__ == '__main__':
    main()
