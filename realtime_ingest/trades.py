import json
import time
import sys
import typing as t

from influxdb_client import InfluxDBClient
import cbpro
from helper.coinbase import get_usd_product_ids

from settings import influx_db as influx_db_settings
from realtime_ingest.sink import RecordSink, BatchingSink, InfluxDBTradeSink
from realtime_ingest.tasks import replay

EXCHANGE_NAME = 'coinbasepro'


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
        self.catching_up[product] = not needs_catch_up
        if needs_catch_up:
            print(f'catching up {product} {watermark}->{trade_id}')
            gap = catchup(product, watermark, trade_id)
            for item in gap:
                self.last_checkpoint = min(self.last_checkpoint, item['time'])
                self.sink.send(item)
        self.sink.send(trade)
        self.watermarks[product] = trade_id
        self.checkpoint_timestamp = max(trade['time'],
                                        self.checkpoint_timestamp)
        if not self.replayed_missed_tasks:
            if all_caught_up:
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
    sink = InfluxDBTradeSink(EXCHANGE_NAME, client.write_api(),
                             org_id=influx_db_settings.INFLUX_ORG_ID,
                             org=influx_db_settings.INFLUX_ORG,
                             bucket="trades")
    sink = BatchingSink(32, sink)
    # Characteristics for storing watermarks: high availability only
    # If watermarks are zero, eventually downloads the DB in O(1) space and O(n) time.
    with open('watermarks.json', 'r') as f:
        watermarks = json.load(f)
    while True:
        try:
            client = TradesWebsocketClient(sink, watermarks,
                                           products=products)
            client.start()
            while not client.stop:
                time.sleep(1)
        except KeyboardInterrupt:
            break
        except Exception:
            pass
        finally:
            # catch up from last state
            sink.flush()
            with open('watermarks.json', 'w') as f:
                json.dump(watermarks, f)
            # only if we know these were actually sent to DB
            watermarks = client.watermarks
            # out here so it doesn't wait on keyboard interrupt
            print('howdy')
            client.close()
        time.sleep(1)
    if client.error:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
