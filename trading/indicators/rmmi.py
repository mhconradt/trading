import time
from datetime import timedelta

import numpy as np
import pandas as pd
from influxdb_client import InfluxDBClient

from trading.helper.ttl_cache import ttl_cache
from trading.indicators.market_fraction import MarketFraction


class RelativeMMI:
    def __init__(self, db: InfluxDBClient, market_fraction: MarketFraction,
                 period: timedelta, toleration: timedelta, quote: str):
        self.db = db
        self.period = period
        self.toleration = toleration
        self.quote = quote
        self.market_fraction = market_fraction

    @ttl_cache(seconds=1.)
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
        large_markets = weights[weights > 0.01].index
        multiples = stop_price / start_price
        # only use large markets in index computation
        mmi = weighted_harmonic_mean(multiples[large_markets],
                                     weights[large_markets])
        print(mmi)
        # prevent exploding values
        mmi = 1.001 if 1 < mmi < 1.001 else 0.999 if 1 > mmi > 0.999 else mmi
        base = np.log(mmi)
        base *= np.sign(base)
        indices = np.log(multiples) / base
        return indices


def weighted_harmonic_mean(data: np.array,
                           weights: np.array) -> float:
    return np.sum(weights) / np.sum(weights / data)


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
    r_mmi10 = RelativeMMI(influx, market_fraction, timedelta(minutes=10),
                          timedelta(minutes=1), 'USD')
    r_mmi15 = RelativeMMI(influx, market_fraction, timedelta(minutes=15),
                          timedelta(minutes=1), 'USD')
    for _ in range(60):
        i5 = r_mmi5.compute().rename('rmmi5')
        describe(i5)
        i10 = r_mmi10.compute().rename('rmmi10')
        describe(i10)
        i15 = r_mmi15.compute().rename('rmmi15')
        describe(i15)
        time.sleep(60)


if __name__ == '__main__':
    from trading.settings import influx_db as influx_db_settings

    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    main(influx_client)
