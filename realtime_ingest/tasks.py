from datetime import timedelta

from influxdb_client import Task, TaskCreateRequest, TasksApi, OrganizationsApi


def task_definition(bucket: str, name: str, every: str, offset: str, **kwargs):
    flux = f"""
        option task = {{
            name: "{name}",
            every: {every},
            offset: {offset},
        }}


        high = from(bucket: "trades")
              |> range(start: -task.every)
              |> filter(fn: (r) => r["_measurement"] == "matches")
              |> filter(fn: (r) => r["_field"] == "price")
              |> aggregateWindow(every: task.every, fn: max, timeSrc: "_start")
              |> keep(columns: ["_time", "market", "_value", "exchange"])
              |> set(key: "_measurement", value: "candles_" + string(v: task.every))
              |> set(key: "_field", value: "high")
              |> to(bucket: "{bucket}")
            
            low = from(bucket: "trades")
              |> range(start: -task.every)
              |> filter(fn: (r) => r["_measurement"] == "matches")
              |> filter(fn: (r) => r["_field"] == "price")
              |> aggregateWindow(every: task.every, fn: min, timeSrc: "_start")
              |> keep(columns: ["_time", "market", "_value", "exchange"])
              |> set(key: "_measurement", value: "candles_" + string(v: task.every))
              |> set(key: "_field", value: "low")
              |> to(bucket: "{bucket}")
            
            open = from(bucket: "trades")
              |> range(start: -task.every)
              |> filter(fn: (r) => r["_measurement"] == "matches")
              |> filter(fn: (r) => r["_field"] == "price")
              |> aggregateWindow(every: task.every, fn: first, timeSrc: "_start")
              |> keep(columns: ["_time", "market", "_value", "exchange"])
              |> set(key: "_measurement", value: "candles_" + string(v: task.every))
              |> set(key: "_field", value: "open")
              |> to(bucket: "{bucket}")
            
            close = from(bucket: "trades")
              |> range(start: -task.every)
              |> filter(fn: (r) => r["_measurement"] == "matches")
              |> filter(fn: (r) => r["_field"] == "price")
              |> aggregateWindow(every: task.every, fn: last, timeSrc: "_start")
              |> keep(columns: ["_time", "market", "_value", "exchange"])
              |> set(key: "_measurement", value: "candles_" + string(v: task.every))
              |> set(key: "_field", value: "close")
              |> to(bucket: "{bucket}")
              
            volume = from(bucket: "trades")
              |> range(start: -task.every)
              |> filter(fn: (r) => r["_measurement"] == "matches")
              |> filter(fn: (r) => r["_field"] == "size")
              |> aggregateWindow(every: task.every, fn: sum, timeSrc: "_start")
              |> keep(columns: ["_time", "market", "_value", "exchange"])
              |> set(key: "_measurement", value: "candles_" + string(v: task.every))
              |> set(key: "_field", value: "volume")
              |> to(bucket: "{bucket}")
    """
    return Task(
        flux=flux,
        name=name,
        **kwargs
    )


def create_candles_tasks():
    from settings import influx_db as influx_db_settings
    from influxdb_client import InfluxDBClient
    _influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                             influx_db_settings.INFLUX_TOKEN,
                             org_id=influx_db_settings.INFLUX_ORG_ID,
                             org=influx_db_settings.INFLUX_ORG)
    tasks_api = _influx.tasks_api()
    candles_1m = task_definition('candles', name='candles_1m',
                                 id='candles_1m',
                                 every='1m',
                                 offset='5s',
                                 org_id=influx_db_settings.INFLUX_ORG_ID,
                                 org=influx_db_settings.INFLUX_ORG
                                 )
    tasks_api.create_task(task=candles_1m)
    candles_5m = task_definition('candles', name='candles_5m',
                                 id='candles_5m',
                                 every='5m',
                                 offset='5s',
                                 org_id=influx_db_settings.INFLUX_ORG_ID,
                                 org=influx_db_settings.INFLUX_ORG
                                 )
    tasks_api.create_task(task=candles_5m)
    candles_15m = task_definition('candles', name='candles_15m',
                                  id='candles_15m',
                                  every='15m',
                                  offset='5s',
                                  org_id=influx_db_settings.INFLUX_ORG_ID,
                                  org=influx_db_settings.INFLUX_ORG
                                  )
    tasks_api.create_task(task=candles_15m)
    candles_1h = task_definition('candles', name='candles_1h',
                                 id='candles_1h',
                                 every='1h',
                                 offset='5s',
                                 org_id=influx_db_settings.INFLUX_ORG_ID,
                                 org=influx_db_settings.INFLUX_ORG
                                 )
    tasks_api.create_task(task=candles_1h)
    candles_6h = task_definition('candles', name='candles_6h',
                                 id='candles_6h',
                                 every='6h',
                                 offset='5s',
                                 org_id=influx_db_settings.INFLUX_ORG_ID,
                                 org=influx_db_settings.INFLUX_ORG
                                 )
    tasks_api.create_task(task=candles_6h)
    candles_1d = task_definition('candles', name='candles_1d',
                                 id='candles_1d',
                                 every='1d',
                                 offset='5s',
                                 org_id=influx_db_settings.INFLUX_ORG_ID,
                                 org=influx_db_settings.INFLUX_ORG
                                 )
    tasks_api.create_task(task=candles_1d)


if __name__ == '__main__':
    create_candles_tasks()
