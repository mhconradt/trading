from datetime import timedelta

from influxdb_client import InfluxDBClient
from pandas import DataFrame, Series


class SlidingMomentum:
    def __init__(self, client: InfluxDBClient, frequency: timedelta,
                 periods: int):
        self.client = client
        self.frequency = frequency
        self.periods = periods

    def compute(self) -> DataFrame:
        params = {'_start': -1 * (self.periods + 1) * self.frequency,
                  '_every': self.frequency}
        query = """
            import "date"
            import "math"
            
            offset = duration(v: int(v: date.truncate(t: now(), unit: _every)) - int(v: now()))
            
            from(bucket: "trades")
                |> range(start: _start)
                |> filter(fn: (r) => r["_measurement"] == "matches")
                |> filter(fn: (r) => r["_field"] == "price")
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
        # make sure at least 1 minute of data in period
        lower_bound = data['_start'].min() + timedelta(minutes=1)
        upper_bound = data['_stop'].max() - timedelta(minutes=1)
        return momentum.loc[lower_bound:upper_bound]


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
        mom = SlidingMomentum(influx, timedelta(minutes=1), 5).compute()
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
