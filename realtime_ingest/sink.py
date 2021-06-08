from abc import ABC, abstractmethod
from decimal import Decimal
import typing as t

from influxdb_client import Point
from influxdb_client.client.write_api import WriteApi


class RecordSink(ABC):
    @abstractmethod
    def send(self, record: dict, /) -> None:
        ...

    @abstractmethod
    def send_many(self, records: t.Iterable[dict], /) -> None:
        ...


class BatchingSink(RecordSink):
    def __init__(self, capacity: int, sink: RecordSink):
        self.capacity = capacity
        self.sink = sink
        self._batch = []

    def send(self, record: dict, /) -> None:
        self._batch.append(record)
        if len(self._batch) >= self.capacity:
            self._send_batch()

    def send_many(self, records: t.Iterable[dict]) -> None:
        for record in records:
            self.send(record)

    def _send_batch(self):
        self.sink.send_many(self._batch)
        self._batch = []

    def flush(self):
        if self._batch:
            self._send_batch()


class Printer(RecordSink):
    def send(self, record: dict, /) -> None:
        print(record)

    def send_many(self, records: t.Iterable[dict], /) -> None:
        print(records)


class InfluxDBTradeSink(RecordSink):
    def __init__(self, exchange: str, writer: WriteApi, *args, **kwargs):
        self.exchange = exchange
        self.point_sink = InfluxDBPointSink(writer, *args, **kwargs)

    def send(self, trade: dict, /) -> None:
        point = Point("matches") \
            .tag('exchange', self.exchange) \
            .tag('market', trade['product_id']) \
            .time(trade['time']) \
            .field('price', Decimal(trade['price'])) \
            .field('size', Decimal(trade['size']))
        self.point_sink.send(point)

    def send_many(self, trades: t.Iterable[dict], /) -> None:
        points = []
        for trade in trades:
            p = Point("matches") \
                .tag('exchange', self.exchange) \
                .tag('market', trade['product_id']) \
                .tag('side', trade['side'])\
                .time(trade['time']) \
                .field('price', Decimal(trade['price'])) \
                .field('size', Decimal(trade['size']))
            points.append(p)
        self.point_sink.send_many(points)


class InfluxDBPointSink(RecordSink):
    def __init__(self, writer: WriteApi, *args, **kwargs):
        self.writer = writer
        self._write_args = args
        self._write_kwargs = kwargs

    def send(self, point: Point, /) -> None:
        self.writer.write(*self._write_args, record=point,
                          **self._write_kwargs)

    def send_many(self, points: t.Iterable[Point], /) -> None:
        self.writer.write(*self._write_args, record=points,
                          **self._write_kwargs)


def main() -> None:
    import itertools as it
    import time

    printer = Printer()
    printer.send({'hello': 'world'})

    def record_generator() -> t.Iterator[dict]:
        n = 0
        while True:
            n += 1
            yield {'hello': 'world', 'n': n}

    batched = BatchingSink(3, printer)
    for record in it.islice(record_generator(), 11):
        time.sleep(1)
        batched.send(record)
    time.sleep(1)
    batched.flush()
    for record in it.islice(record_generator(), 3):
        batched.send_many([record] * 4)
        time.sleep(1)
    batched.flush()


if __name__ == '__main__':
    main()
