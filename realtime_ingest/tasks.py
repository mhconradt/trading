import itertools as it
import typing as t
from collections import defaultdict

from influxdb_client import Task, TasksApi


class ReplayHook:
    def __init__(self):
        self.task_api = None
        self.initialized = False
        self.subscriptions = defaultdict(set)

    def initialize(self, task_api: TasksApi) -> None:
        self.task_api = task_api
        self.initialized = True

    def subscribe(self, task_def: "TaskDefinition", to: str):
        """
        Subscribe task to bucket.
        """
        self.subscriptions[to].add(task_def)

    def replay(self, measurement: str, start: str, end: str) -> None:
        assert self.initialized
        for task in self.subscriptions[measurement]:
            task.initialize(self.task_api)
            task.replay(start, end)


replay = ReplayHook()


class TaskDefinition:
    def __init__(self, src: str, name: str, every: str, offset: str, dst: str):
        self.task_api = None
        self.task_kwargs = None
        self.initialized = False
        self.dst = dst
        self.name = name
        self.every = every
        self.offset = offset
        self.src = src
        replay.subscribe(self, "matches")

    def initialize(self, task_api: TasksApi, **kwargs) -> None:
        self.task_api = task_api
        self.task_kwargs = kwargs
        self.initialized = True

    def task_id(self) -> t.Optional[str]:
        assert self.initialized
        tasks = self.task_api.find_tasks(name=self.name)
        if not tasks:
            return None
        task = tasks[0]
        return task.id

    def exists(self) -> bool:
        assert self.initialized
        tasks = self.task_api.find_tasks(name=self.name)
        return bool(tasks)

    def replay(self, start: str, end: str) -> None:
        assert self.initialized
        if not self.exists():
            return None
        task_id = self.task_id()
        runs = self.task_api.get_runs(task_id, after_time=start,
                                      before_time=end)
        for run in it.islice(runs, 0, 5):  # hack to only replay last five runs
            print('replaying', run.id)
            self.task_api.retry_run(task_id, run.id)

    def create(self) -> None:
        assert self.initialized
        if self.exists():
            return None
        flux = f"""
                option task = {{
                    name: "{self.name}",
                    every: {self.every},
                    offset: {self.offset},
                }}


                high = from(bucket: "{self.src}")
                      |> range(start: -task.every)
                      |> filter(fn: (r) => r["_measurement"] == "matches")
                      |> filter(fn: (r) => r["_field"] == "price")
                      |> keep(columns: ["_time", "market", "_value", "exchange", "base", "quote"])
                      |> aggregateWindow(every: task.every, fn: max, timeSrc: "_start")
                      |> set(key: "_measurement", value: "candles_" + string(v: task.every))
                      |> set(key: "_field", value: "high")
                      |> to(bucket: "{self.dst}")

                    low = from(bucket: "{self.src}")
                      |> range(start: -task.every)
                      |> filter(fn: (r) => r["_measurement"] == "matches")
                      |> filter(fn: (r) => r["_field"] == "price")
                      |> keep(columns: ["_time", "market", "_value", "exchange", "base", "quote"])
                      |> aggregateWindow(every: task.every, fn: min, timeSrc: "_start")
                      |> set(key: "_measurement", value: "candles_" + string(v: task.every))
                      |> set(key: "_field", value: "low")
                      |> to(bucket: "{self.dst}")

                    open = from(bucket: "{self.src}")
                      |> range(start: -task.every)
                      |> filter(fn: (r) => r["_measurement"] == "matches")
                      |> filter(fn: (r) => r["_field"] == "price")
                      |> keep(columns: ["_time", "market", "_value", "exchange", "base", "quote"])
                      |> aggregateWindow(every: task.every, fn: first, timeSrc: "_start")
                      |> set(key: "_measurement", value: "candles_" + string(v: task.every))
                      |> set(key: "_field", value: "open")
                      |> to(bucket: "{self.dst}")

                    close = from(bucket: "{self.src}")
                      |> range(start: -task.every)
                      |> filter(fn: (r) => r["_measurement"] == "matches")
                      |> filter(fn: (r) => r["_field"] == "price")
                      |> keep(columns: ["_time", "market", "_value", "exchange", "base", "quote"])
                      |> aggregateWindow(every: task.every, fn: last, timeSrc: "_start")
                      |> set(key: "_measurement", value: "candles_" + string(v: task.every))
                      |> set(key: "_field", value: "close")
                      |> to(bucket: "{self.dst}")

                    volume = from(bucket: "{self.src}")
                      |> range(start: -task.every)
                      |> filter(fn: (r) => r["_measurement"] == "matches")
                      |> filter(fn: (r) => r["_field"] == "size")
                      |> keep(columns: ["_time", "market", "_value", "exchange", "base", "quote"])
                      |> aggregateWindow(every: task.every, fn: sum, timeSrc: "_start")
                      |> set(key: "_measurement", value: "candles_" + string(v: task.every))
                      |> set(key: "_field", value: "volume")
                      |> to(bucket: "{self.dst}")

                    quote_volume = from(bucket: "{self.src}")
                      |> range(start: -task.every)
                      |> filter(fn: (r) => r["_measurement"] == "matches")
                      |> filter(fn: (r) => r["_field"] == "price" or r["_field"] == "size")
                      |> pivot(rowKey: ["_time", "market", "exchange", "base", "quote"],
                               columnKey: ["_field"],
                               valueColumn: "_value")
                      |> map(fn: (r) => {'({ r with _value: r["price"] * r["size"]})'})
                      |> keep(columns: ["_time", "market", "_value", "exchange", "base", "quote"])
                      |> aggregateWindow(every: task.every, fn: sum, timeSrc: "_start")
                      |> set(key: "_measurement", value: "candles_" + string(v: task.every))
                      |> set(key: "_field", value: "quote_volume")
                      |> to(bucket: "{self.dst}")
            """
        task = Task(
            flux=flux,
            name=self.name,
            **self.task_kwargs
        )
        self.task_api.create_task(task)
        return None


candles_1m = TaskDefinition('level1', name='candles_1m', every='1m',
                            offset='1s', dst='candles')


def main():
    from settings import influx_db as influx_db_settings
    from influxdb_client import InfluxDBClient
    _influx = InfluxDBClient(influx_db_settings.INFLUX_URL,
                             influx_db_settings.INFLUX_TOKEN,
                             org_id=influx_db_settings.INFLUX_ORG_ID,
                             org=influx_db_settings.INFLUX_ORG)
    create_all(_influx, influx_db_settings.INFLUX_ORG_ID,
               org=influx_db_settings.INFLUX_ORG)


def create_all(_influx, org_id,
               org):
    tasks_api = _influx.tasks_api()
    candles_1m.initialize(tasks_api, id='candles_1m', org_id=org_id, org=org)
    candles_1m.create()


if __name__ == '__main__':
    main()
