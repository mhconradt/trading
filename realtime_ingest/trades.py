import sys
import time
import typing as t
from datetime import timedelta

import cbpro
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import ASYNCHRONOUS

from helper.coinbase import get_usd_product_ids, get_server_time
from realtime_ingest.sink import RecordSink, BatchingSink, InfluxDBTradeSink
from realtime_ingest.tasks import replay
from realtime_ingest.watermarks import watermarks_at_time
from settings import influx_db as influx_db_settings

EXCHANGE_NAME = 'coinbasepro'


def initialize_watermarks(influx_client: InfluxDBClient,
                          bucket: str,
                          products: t.List[str]) -> dict:
    query_api = influx_client.query_api()
    window = timedelta(days=2)
    result = query_api.query_data_frame("""
        from(bucket: bucket)
            |> range(start: window_start)
            |> filter(fn: (r) => r["_measurement"] == "matches")
            |> filter(fn: (r) => r["_field"] == "trade_id")
            |> filter(fn: (r) => r["exchange"] == "coinbasepro")
            |> aggregateWindow(every: 2d, fn: max, createEmpty: false)
            |> yield(name: "watermark")
    """, data_frame_index=['market'], params={'bucket': bucket,
                                              'window_start': -window})
    watermarks = result['_value'].to_dict()
    if not watermarks:  # starting from scratch
        return watermarks_at_time(get_server_time() - window, products)
    return {market: watermark for market, watermark in watermarks.items()
            if market in products}


def catchup(product: str, frm: int, to: int) -> t.Iterable[dict]:
    client = cbpro.PublicClient()
    for trade in client.get_product_trades(product):
        trade_id = trade['trade_id']
        if trade_id >= to:
            continue
        elif trade_id <= frm:
            break
        else:
            trade['product_id'] = product
            yield trade


class TradesWebsocketClient(cbpro.WebsocketClient):
    def __init__(self, sink: RecordSink,
                 watermarks: t.Optional[dict],
                 *args,
                 **kwargs):
        if 'channels' in kwargs:
            del kwargs['channels']
        super().__init__(*args, channels=['matches', 'heartbeat'], **kwargs)
        self.sink = sink
        # catchup aggregates
        self.watermarks = dict() if watermarks is None else watermarks
        # start of period to replay
        # when to trigger the replays
        self.replayed_missed_tasks = False
        self.catching_up = {market: True for market in watermarks}
        self.last_checkpoint = 'Z'
        self.checkpoint_timestamp = ''

    def on_open(self):
        pass

    def on_message(self, msg: dict) -> None:
        msg_type = msg['type']
        if msg_type not in {'match', 'last_match'}:
            return None
        trade = msg  # message is a trade now
        product = trade['product_id']
        trade_id = trade['trade_id']
        watermark = self.watermarks.get(product)
        # all markets are now being processed in order
        needs_catch_up = watermark and trade_id > watermark + 1
        all_caught_up = not (any(self.catching_up.values()) or needs_catch_up)
        if not self.catching_up[product] and needs_catch_up:
            self.replayed_missed_tasks = False
        self.catching_up[product] = needs_catch_up
        if needs_catch_up:
            print(f'catching up {product} {watermark}->{trade_id}')
            gap = catchup(product, watermark, trade_id)
            for item in gap:
                self.last_checkpoint = min(self.last_checkpoint, item['time'])
                self.sink.send(item)
            print(f'caught up {product}')
        self.sink.send(trade)
        self.watermarks[product] = trade_id
        self.checkpoint_timestamp = max(trade['time'],
                                        self.checkpoint_timestamp)
        if not self.replayed_missed_tasks:
            if all_caught_up:
                print('replaying')
                replay.replay("trades", self.last_checkpoint,
                              self.checkpoint_timestamp)
                self.replayed_missed_tasks = True


def main() -> None:
    products = get_usd_product_ids()
    client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                            influx_db_settings.INFLUX_TOKEN,
                            org_id=influx_db_settings.INFLUX_ORG_ID,
                            org=influx_db_settings.INFLUX_ORG)
    replay.initialize(client.tasks_api())
    watermarks = initialize_watermarks(client, "trades", products)
    sink = InfluxDBTradeSink(EXCHANGE_NAME,
                             client.write_api(write_options=ASYNCHRONOUS),
                             org_id=influx_db_settings.INFLUX_ORG_ID,
                             org=influx_db_settings.INFLUX_ORG,
                             bucket="trades")
    sink = BatchingSink(4, sink)
    while True:
        try:
            client = TradesWebsocketClient(sink, watermarks,
                                           products=products)
            client.start()
            while not client.stop:
                time.sleep(5)
        except KeyboardInterrupt:
            break
        finally:
            # catch up from last state
            sink.flush()
            watermarks = initialize_watermarks(client, "trades",
                                               products)
            # out here so it doesn't wait on keyboard interrupt
            print('howdy')
            client.close()  # this can block
        time.sleep(1)
    if client.error:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
