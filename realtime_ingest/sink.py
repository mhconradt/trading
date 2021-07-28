import typing as t
from abc import ABC, abstractmethod
from datetime import timedelta
from decimal import Decimal

import dateutil.parser
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
    def __init__(self, sink: RecordSink, capacity: int):
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


class InfluxDBTickerSink(RecordSink):
    def __init__(self, exchange: str, writer: WriteApi, *args, **kwargs):
        self.exchange = exchange
        self.point_sink = InfluxDBPointSink(writer, *args, **kwargs)

    def send(self, ticker: dict, /) -> None:
        point = self.build_point(ticker)
        self.point_sink.send(point)

    def send_many(self, tickers: t.Iterable[dict], /) -> None:
        points = []
        for trade in tickers:
            p = self.build_point(trade)
            points.append(p)
        self.point_sink.send_many(points)

    def build_point(self, ticker: dict) -> Point:
        product = ticker['product_id']
        timestamp = dateutil.parser.parse(ticker['time'])
        return Point("tickers") \
            .tag('exchange', self.exchange) \
            .tag('market', product) \
            .time(timestamp) \
            .field('bid', Decimal(ticker['best_bid'])) \
            .field('ask', Decimal(ticker['best_ask']))


class InfluxDBTradeSink(RecordSink):
    def __init__(self, exchange: str, writer: WriteApi, *args, **kwargs):
        self.exchange = exchange
        self.point_sink = InfluxDBPointSink(writer, *args, **kwargs)
        self.product_timestamps = dict()
        self.product_anchors = dict()

    def send(self, trade: dict, /) -> None:
        point = self.build_point(trade)
        self.point_sink.send(point)

    def build_point(self, trade: dict) -> Point:
        product = trade['product_id']
        timestamp = dateutil.parser.parse(trade['time'])
        trade_id = trade['trade_id']
        if self.product_timestamps.get(product) != timestamp:
            self.product_anchors[product] = trade_id
            self.product_timestamps[product] = timestamp
        anchor = self.product_anchors[product]
        salt = trade_id - anchor
        # salting timestamps serves two purposes:
        # 1. Ensures trades with same timestamp are not dropped.
        # 2. Encodes order of execution in order of timestamps.
        # essentially mixes in some of our own magic logic sauce into the data.
        return Point("matches") \
            .tag('exchange', self.exchange) \
            .tag('market', trade['product_id']) \
            .tag('side', trade['side']) \
            .time(timestamp + timedelta(microseconds=salt)) \
            .field('price', Decimal(trade['price'])) \
            .field('size', Decimal(trade['size'])) \
            .field('trade_id', int(trade_id))

    def send_many(self, trades: t.Iterable[dict], /) -> None:
        points = []
        for trade in trades:
            p = self.build_point(trade)
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

    batched = BatchingSink(printer, 3)
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
