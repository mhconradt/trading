from datetime import timedelta

from influxdb_client import InfluxDBClient
from pandas import DataFrame, Series


class SlidingMomentum:
    def __init__(self, client: InfluxDBClient, exchange: str, periods: int,
                 frequency: timedelta):
        self.exchange = exchange
        self.client = client
        self.frequency = frequency
        self.periods = periods

    def compute(self) -> DataFrame:
        params = {'_start': -1 * (self.periods + 1) * self.frequency,
                  '_every': self.frequency,
                  '_exchange': self.exchange}
        query = """
            import "date"
            import "math"
            
            offset = duration(v: int(v: now()) - int(v: date.truncate(t: now(), unit: _every)))
            
            from(bucket: "trades")
                |> range(start: _start)
                |> filter(fn: (r) => r["_measurement"] == "matches")
                |> filter(fn: (r) => r["_field"] == "price")
                |> filter(fn: (r) => r["exchange"] == _exchange)
                |> keep(columns: ["market", "_time", "exchange", "_value"])
                |> window(every: _every, period: _every, offset: offset)
                |> last()
                |> duplicate(column: "_start", as: "_time")
                |> window(every: inf)
                |> map(fn: (r) => ({r with _value: math.log(x: r["_value"])}))
                |> difference()
                |> map(fn: (r) => ({r with _value: math.expm1(x: r["_value"])}))
                |> yield(name: "momentum")
        """
        query_api = self.client.query_api()
        data = query_api.query_data_frame(query, params=params,
                                          data_frame_index=['market', '_time'])
        momentum = data['_value'].unstack('market')
        return momentum


def present_trend(momentum: DataFrame) -> Series:
    factors = (momentum + 1).where(momentum > 0., 0.)
    return factors.iloc[::-1].cumprod().max() - 1.


if __name__ == '__main__':
    import settings.influx_db as influx_db_settings

    influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                            influx_db_settings.INFLUX_TOKEN,
                            org_id=influx_db_settings.INFLUX_ORG_ID,
                            org=influx_db_settings.INFLUX_ORG)
    import time

    while True:
        start = time.time()
        mom = SlidingMomentum(influx, 'coinbasepro', 5,
                              timedelta(minutes=1)).compute()
        trend = present_trend(mom)
        accelerating = mom.iloc[-1] > mom.iloc[-2]
        mask = (trend > 0.005) & accelerating
        buys = trend[mask] - 0.005
        fraction = mom.iloc[-1] / mom.iloc[-2]
        fraction = fraction.where(fraction > 0, 0).where(fraction < 1, 1)
        print(fraction.sort_values(ascending=False).head(16))
        print((buys / buys.sum()).sort_values(ascending=False))
        end = time.time()
        time.sleep(10 - (end - start))
