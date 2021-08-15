import sys
import time
import typing as t
from datetime import timedelta

import cbpro
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

from helper.coinbase import PublicClient
from realtime_ingest.sink import RecordSink, InfluxDBTradeSink, \
    InfluxDBTickerSink, BatchingSink, InfluxDBPointSink
from realtime_ingest.tasks import replay, create_all
from settings import influx_db as influx_db_settings

EXCHANGE_NAME = 'coinbasepro'


def initialize_watermarks(influx_client: InfluxDBClient,
                          bucket: str,
                          products: t.List[str]) -> dict:
    window = timedelta(minutes=5)
    query_api = influx_client.query_api()
    params = {'bucket': bucket,
              'start': -window}
    result = query_api.query_data_frame("""
        from(bucket: bucket)
            |> range(start: start)
            |> filter(fn: (r) => r["_measurement"] == "matches")
            |> filter(fn: (r) => r["_field"] == "trade_id")
            |> filter(fn: (r) => r["exchange"] == "coinbasepro")
            |> keep(columns: ["market", "_value", "_time"])
            |> aggregateWindow(every: 2d, fn: max, createEmpty: false)
            |> yield(name: "watermark")
    """, data_frame_index=['market'], params=params)
    watermarks = result['_value'].to_dict()
    watermarks = {market: watermark for market, watermark in watermarks.items()
                  if market in products}
    return watermarks


def catchup(product: str, frm: int, to: int) -> t.Iterable[dict]:
    client = PublicClient()
    for trade in client.get_product_trades(product):
        trade_id = trade['trade_id']
        if not (to - trade_id) % 1000:
            time.sleep(0.1)  # TODO: Figure out rate limiting this endpoint
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
            except (Exception,) as e:
                print(e)
                self.stop = True


class TickerHandler(MessageHandler):
    def __init__(self, sink: RecordSink):
        self.sink = sink

    def on_message(self, msg: dict) -> None:
        if 'time' in msg:
            self.sink.send(msg)


class TradesMessageHandler(MessageHandler):
    def __init__(self, sink: RecordSink,
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
        watermark = self.watermarks.get(product, trade_id)
        # all markets are now being processed in order
        needs_catch_up = watermark and trade_id > watermark + 1
        all_caught_up = not (
                any(self.catching_up.values()) or needs_catch_up)
        if not self.catching_up.get(product, False) and needs_catch_up:
            self.replayed_missed_tasks = False
        self.catching_up[product] = needs_catch_up
        if needs_catch_up:
            self.sink.capacity = trade_id - watermark
            print(f'catching up {product} {watermark}->{trade_id}')
            gap = catchup(product, watermark, trade_id)
            for item in gap:
                self.checkpoint_start = min(self.checkpoint_start,
                                            item['time'])
                self.sink.send(item)
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
    quote_currencies = {'USD', 'USDT', 'USDC'}
    client = PublicClient()
    influx_client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                                   influx_db_settings.INFLUX_TOKEN,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG)
    replay.initialize(influx_client.tasks_api())
    create_all(influx_client, org_id=influx_db_settings.INFLUX_ORG_ID,
               org=influx_db_settings.INFLUX_ORG)
    writer = influx_client.write_api(write_options=SYNCHRONOUS)
    point_sink = InfluxDBPointSink(writer,
                                   org_id=influx_db_settings.INFLUX_ORG_ID,
                                   org=influx_db_settings.INFLUX_ORG,
                                   bucket="level1")
    point_sink = BatchingSink(point_sink, 32)
    trade_sink = InfluxDBTradeSink(EXCHANGE_NAME, point_sink)
    ticker_sink = InfluxDBTickerSink(EXCHANGE_NAME, point_sink)
    while True:
        products = [product['id'] for product in client.get_products()
                    if product['quote_currency'] in quote_currencies]
        watermarks = initialize_watermarks(influx_client, "level1",
                                           products)
        trade_handler = TradesMessageHandler(trade_sink, watermarks)
        ticker_handler = TickerHandler(ticker_sink)
        ws_client = RouterClient({trade_handler: ['match', 'last_match'],
                                  ticker_handler: ['ticker'], },
                                 channels=['matches', 'ticker'],
                                 products=products)
        try:
            ws_client.start()
            while not ws_client.stop:
                time.sleep(1)
        except KeyboardInterrupt:
            break
        finally:
            # catch up from last state
            # out here so it doesn't wait on keyboard interrupt
            print('howdy')
            ws_client.close()  # this can block
    if ws_client.error:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
