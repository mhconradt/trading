import time
from datetime import timedelta

import numpy as np
import pandas as pd
from influxdb_client import InfluxDBClient

from trading.helper.functions import overlapping_labels
from trading.helper.ttl_cache import ttl_cache
from trading.indicators.market_fraction import MarketFraction


def std(a: np.array, w: np.array) -> float:
    w /= w.sum()
    expected_value = (a * w).sum()
    return np.sqrt(np.sum(w * (a - expected_value) ** 2))


def compute_index(multiples: np.array, weights: np.array) -> np.array:
    multiples, weights = overlapping_labels(multiples.dropna(), weights)
    # only use large markets in index computation
    log_multiples = np.log(multiples)
    sigma = std(log_multiples, weights)
    mean = np.average(log_multiples, weights=weights)
    indices = (log_multiples - mean) / sigma
    return indices


class RelativeMMI:
    def __init__(self, db: InfluxDBClient, market_fraction: MarketFraction,
                 period: timedelta, toleration: timedelta, quote: str):
        self.db = db
        self.period = period
        self.toleration = toleration
        self.quote = quote
        self.market_fraction = market_fraction

    @ttl_cache(seconds=2.)
    def compute(self) -> pd.Series:
        price_query = """
            from(bucket: "level1")
                |> range(start: start, stop: stop)
                |> filter(fn: (r) => r["_measurement"] == "matches")
                |> filter(fn: (r) => r["quote"] == quote)
                |> filter(fn: (r) => r["_field"] == "price")
                |> keep(columns: ["_time", "market", "_value"])
                |> last()
                |> yield(name: "price")
        """
        start_price_parameters = {'start': -(self.period + self.toleration),
                                  'stop': -self.period,
                                  'quote': self.quote}
        query_api = self.db.query_api()
        start_df = query_api.query_data_frame(price_query,
                                              params=start_price_parameters,
                                              data_frame_index=['market'])
        start_price = start_df['_value']
        stop_price_parameters = {'start': -self.toleration,
                                 'stop': timedelta(0),
                                 'quote': self.quote}
        stop_df = query_api.query_data_frame(price_query,
                                             params=stop_price_parameters,
                                             data_frame_index=['market'])
        stop_price = stop_df['_value']
        weights = self.market_fraction.compute()
        multiples = stop_price / start_price
        return compute_index(multiples, weights)


def describe(indices: pd.Series) -> None:
    print("#" * 5, indices.name, "#" * 5)
    print(f"- {indices.idxmin()}: {indices.min()}")
    print(f"+ {indices.idxmax()}: {indices.max()}")
    print(indices[['SOL-USD', 'ETH-USD', 'BTC-USD', 'ADA-USD', 'ICP-USD']])


def main(influx: InfluxDBClient) -> None:
    market_fraction = MarketFraction(influx, 60, timedelta(minutes=1),
                                     'USD')
    r_mmi5 = RelativeMMI(influx, market_fraction, timedelta(minutes=5),
                         timedelta(minutes=1), 'USD')
    r_mmi3 = RelativeMMI(influx, market_fraction, timedelta(minutes=3),
                         timedelta(minutes=1), 'USD')
    while True:
        i5 = r_mmi5.compute()
        i3 = r_mmi3.compute()
        df = pd.DataFrame({'rmmi3': i3, 'rmmi5': i5})
        print(df.describe())
        extremes = pd.DataFrame({'idxmin': df.idxmin(), 'min': df.min(),
                                 'idxmax': df.idxmax(), 'max': df.max(), })
        print(extremes)
        print(df.loc[['SOL-USD', 'ETH-USD', 'BTC-USD', 'ADA-USD', 'ICP-USD']])
        time.sleep(30)


if __name__ == '__main__':
    from trading.settings import influx_db as influx_db_settings

    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    main(influx_client)
