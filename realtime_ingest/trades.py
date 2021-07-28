import sys
import time
import typing as t
from datetime import timedelta

import cbpro
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

from helper.coinbase import get_usd_product_ids, get_server_time, \
    PublicClient
from realtime_ingest.sink import RecordSink, InfluxDBTradeSink, \
    InfluxDBTickerSink, BatchingSink
from realtime_ingest.tasks import replay, create_all
from realtime_ingest.watermarks import watermarks_at_time
from settings import influx_db as influx_db_settings
import sys
import time
import typing as t
from datetime import timedelta

import cbpro
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

from helper.coinbase import get_usd_product_ids, get_server_time, \
    PublicClient
from realtime_ingest.sink import RecordSink, InfluxDBTradeSink, \
    InfluxDBTickerSink, BatchingSink
from realtime_ingest.tasks import replay, create_all
from realtime_ingest.watermarks import watermarks_at_time
from settings import influx_db as influx_db_settings

EXCHANGE_NAME = 'coinbasepro'


def initialize_watermarks(influx_client: InfluxDBClient,
                          bucket: str,
                          products: t.List[str]) -> dict:
    query_api = influx_client.query_api()
    window = timedelta(minutes=15)
    result = query_api.query_data_frame("""
        from(bucket: bucket)
            |> range(start: window_start)
            |> filter(fn: (r) => r["_measurement"] == "matches")
            |> filter(fn: (r) => r["_field"] == "trade_id")
            |> filter(fn: (r) => r["exchange"] == "coinbasepro")
            |> keep(columns: ["market", "_value", "_time"])
            |> aggregateWindow(every: 2d, fn: max, createEmpty: false)
            |> yield(name: "watermark")
    """, data_frame_index=['market'], params={'bucket': bucket,
                                              'window_start': -window})
    if not len(result):  # starting from scratch
        return watermarks_at_time(get_server_time() - window, products)
    watermarks = result['_value'].to_dict()
    watermarks = {market: watermark for market, watermark in watermarks.items()
                  if market in products}
    remaining = set(products).difference(watermarks)
    remaining_watermarks = watermarks_at_time(get_server_time() - window,
                                              remaining)
    return {**watermarks, **remaining_watermarks}


def catchup(product: str, frm: int, to: int) -> t.Iterable[dict]:
    client = PublicClient()
    for trade in client.get_product_trades(product):
        trade_id = trade['trade_id']
        if trade_id >= to:
            continue
        elif trade_id <= frm:
            break
        else:
            trade['product_id'] = product
            yield trade


from abc import ABC, abstractmethod
from collections import defaultdict


class MessageHandler(ABC):
    @abstractmethod
    def on_message(self, msg: dict) -> None:
        pass


class RouterClient(cbpro.WebsocketClient):
    def __init__(self, handlers: t.Dict[MessageHandler, t.List[str]],
                 **kwargs):
        channels = set(kwargs.get('channels', []))
        channels.add('heartbeat')
        kwargs['channels'] = list(channels)
        super().__init__(**kwargs)
        subscriptions = defaultdict(list)
        for handler, channels in handlers.items():
            for channel in channels:
                subscriptions[channel].append(handler)
        self.subscriptions = subscriptions
        print(self.subscriptions)

    def on_message(self, msg: dict) -> None:
        for handler in self.subscriptions[msg['type']]:
            try:  # process message
                handler.on_message(msg)
            except (Exception,):
                self.stop = True


class TickerHandler(MessageHandler):
    def __init__(self, sink: RecordSink):
        self.sink = sink

    def on_message(self, msg: dict) -> None:
        self.sink.send(msg)


class TradesMessageHandler(MessageHandler):
    def __init__(self, sink: BatchingSink,
                 watermarks: t.Optional[dict]):
        self.sink = sink
        # catchup aggregates
        self.watermarks = dict() if watermarks is None else watermarks
        # start of period to replay
        # when to trigger the replays
        self.replayed_missed_tasks = False
        self.catching_up = {market: True for market in watermarks}
        self.checkpoint_start = 'Z'
        self.checkpoint_end = ''

    def on_message(self, msg: dict) -> None:
        trade = msg  # message is a trade now
        product = trade['product_id']
        trade_id = trade['trade_id']
        watermark = self.watermarks.get(product)
        # all markets are now being processed in order
        needs_catch_up = watermark and trade_id > watermark + 1
        all_caught_up = not (
                any(self.catching_up.values()) or needs_catch_up)
        if not self.catching_up[product] and needs_catch_up:
            self.replayed_missed_tasks = False
        self.catching_up[product] = needs_catch_up
        if needs_catch_up:
            prev_capacity = self.sink.capacity
            self.sink.capacity = trade_id - watermark
            print(f'catching up {product} {watermark}->{trade_id}')
            gap = catchup(product, watermark, trade_id)
            for item in gap:
                self.checkpoint_start = min(self.checkpoint_start,
                                            item['time'])
                self.sink.send(item)
            self.sink.capacity = prev_capacity
            print(f'caught up {product}')
        self.sink.send(trade)
        self.watermarks[product] = trade_id
        self.checkpoint_start = min(self.checkpoint_start, trade['time'])
        self.checkpoint_end = max(trade['time'],
                                  self.checkpoint_end)
        if not self.replayed_missed_tasks:
            if all_caught_up and self.checkpoint_start != 'Z':
                print('replaying')
                replay.replay("matches", self.checkpoint_start,
                              self.checkpoint_end)
                self.replayed_missed_tasks = True
                self.checkpoint_start = 'Z'


def main() -> None:
    products = get_usd_product_ids()
    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    replay.initialize(influx_client.tasks_api())
    create_all(influx_client, org_id=influx_db_settings.INFLUX_ORG_ID,
               org=influx_db_settings.INFLUX_ORG)
    watermarks = initialize_watermarks(influx_client, "trades", products)
    writer = influx_client.write_api(write_options=SYNCHRONOUS)
    trade_sink = InfluxDBTradeSink(EXCHANGE_NAME,
                                   writer,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG,
                                   bucket="trades")
    ticker_sink = InfluxDBTickerSink(EXCHANGE_NAME,
                                     writer,
                                     org_id=influx_db_settings.INFLUX_ORG_ID,
                                     org=influx_db_settings.INFLUX_ORG,
                                     bucket="trades")
    while True:
        trade_handler = TradesMessageHandler(BatchingSink(trade_sink, 1),
                                             watermarks)
        ticker_handler = TickerHandler(ticker_sink)
        trade_client = RouterClient({trade_handler: ['match', 'last_match'],
                                     ticker_handler: ['ticker']},
                                    channels=['matches'],
                                    products=products)
        try:
            trade_client.start()
            while not trade_client.stop:
                time.sleep(5)
        except KeyboardInterrupt:
            break
        finally:
            # catch up from last state
            watermarks = initialize_watermarks(influx_client, "trades",
                                               products)
            # out here so it doesn't wait on keyboard interrupt
            print('howdy')
            trade_client.close()  # this can block
        time.sleep(1)
    if trade_client.error:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
