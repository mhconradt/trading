import typing as t
from datetime import datetime

import dateutil.parser
import requests

from trading.coinbase.helper import wait_for_public_rate_limit, PublicClient

public_client = PublicClient()


def watermarks_at_time(start: datetime, products: t.Iterable[str]) -> dict:
    watermarks = {}
    for product in products:
        trade_id = find_trade_id(product, start)
        if trade_id:
            watermarks[product] = trade_id
    return watermarks


def find_trade_id_cursor(product_id: str, to: datetime, start: int,
                         end: int) -> int:
    bisector = (start + end) // 2
    if bisector == start:
        return start
    elif bisector == end:
        return end
    elif bisector == 1:
        return 1
    bisector_timestamp = get_timestamp(product_id, bisector)
    if bisector_timestamp > to:
        return find_trade_id_cursor(product_id, to, start, bisector)
    else:
        return find_trade_id_cursor(product_id, to, bisector, end)


def find_trade_id(product_id: str, to: datetime) -> int:
    trades = public_client.get_product_trades(product_id)
    try:
        trade_id = next(trades)['trade_id']
        return find_trade_id_cursor(product_id, to, 1, trade_id)
    except StopIteration:
        return 0


def get_timestamp(product_id: str, trade_id: int) -> datetime:
    params = {'before': trade_id - 1, 'after': trade_id + 1}
    wait_for_public_rate_limit()
    trades = requests.get(
        f"https://api.pro.coinbase.com/products/{product_id}/trades",
        params).json()
    trade, = trades
    return dateutil.parser.parse(trade['time'])
