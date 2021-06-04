import time
import sys
import typing as t

from influxdb_client import InfluxDBClient
import cbpro
from helper.coinbase import get_usd_products

from settings import influx_db as influx_db_settings
from realtime_ingest.sink import RecordSink, BatchingSink, InfluxDBTradeSink

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
                 watermarks: t.Optional[dict] = None,
                 *args,
                 **kwargs):
        if 'channels' in kwargs:
            del kwargs['channels']
        super().__init__(*args, channels=['matches'], **kwargs)
        self.sink = sink
        self.watermarks = dict() if watermarks is None else watermarks

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
        if watermark and trade_id > watermark + 1:
            print(f'catching up {product} {watermark}->{trade_id}')
            gap = catchup(product, watermark, trade_id)
            self.sink.send_many(gap)
        self.sink.send(trade)
        self.watermarks[product] = trade_id


def main() -> None:
    products = get_usd_products()
    client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                            influx_db_settings.INFLUX_TOKEN,
                            org=influx_db_settings.INFLUX_ORG)
    sink = InfluxDBTradeSink(EXCHANGE_NAME, client.write_api(),
                             org=influx_db_settings.INFLUX_ORG,
                             bucket=influx_db_settings.INFLUX_BUCKET)
    sink = BatchingSink(64, sink)
    # TODO: Persist watermarks
    # If watermarks are zero, eventually downloads the DB in O(1) space and O(n) time.
    watermarks = {}
    while True:
        try:
            client = TradesWebsocketClient(sink, watermarks, products=products)
            client.start()
            while not client.stop:
                time.sleep(1)
        except KeyboardInterrupt:
            break
        finally:
            print('howdy')
            client.close()
            # catch up from last state
            sink.flush()
            # only if we know these were actually sent to DB
            watermarks = client.watermarks
        time.sleep(15)

    # maybe persist watermarks elsewhere?

    if client.error:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
