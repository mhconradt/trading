from influxdb_client import InfluxDBClient
import pandas as pd

from settings import influx_db as influx_db_settings


def main() -> None:
    client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                            influx_db_settings.INFLUX_TOKEN,
                            org=influx_db_settings.INFLUX_ORG)
    ...
