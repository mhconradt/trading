import sys
from datetime import timedelta

import pandas as pd
from influxdb_client import InfluxDBClient

from trading.helper.ttl_cache import ttl_cache
from trading.indicators.candles import CandleSticks


class ATR:
    def __init__(self, db: InfluxDBClient, periods: int, frequency: timedelta,
                 quote: str):
        self.candles = CandleSticks(db, 2 * periods + 1, frequency, quote)
        self.periods = periods

    @ttl_cache(seconds=11.)
    def compute(self) -> pd.Series:
        candles = self.candles.compute()
        tr = true_range(candles)
        return tr.ewm(span=self.periods).mean().iloc[-1]


def true_range(candles: pd.DataFrame) -> pd.DataFrame:
    candles = candles.unstack('market')
    high, low, close = candles.high, candles.low, candles.close
    _range = high - low
    non_zero_range = _range.where(_range != 0., sys.float_info.epsilon)
    previous_close = close.shift(1)
    min_delta, max_delta = low - previous_close, high - previous_close
    abs_min_delta, abs_max_delta = min_delta.abs(), max_delta.abs()
    max_abs_difference = abs_max_delta.where(abs_max_delta > abs_min_delta,
                                             abs_min_delta)
    _true_range = max_abs_difference.where(max_abs_difference > non_zero_range,
                                           non_zero_range)
    return _true_range


def main(influx: InfluxDBClient) -> None:
    indicator = ATR(influx, periods=14, frequency=timedelta(minutes=1),
                    quote='USD')
    ranges = indicator.compute()
    print(ranges)


if __name__ == '__main__':
    from trading.settings import influx_db as influx_db_settings

    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    main(influx_client)
