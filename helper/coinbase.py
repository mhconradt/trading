import json
import typing as t
from datetime import datetime

import cbpro
import dateutil.parser


def get_usd_products() -> t.List[dict]:
    cb_client = cbpro.PublicClient()
    products = cb_client.get_products()
    return [product for product in products if
            product['quote_currency'] == 'USD']


def get_usd_product_ids() -> t.List[str]:
    return [product['id'] for product in get_usd_products()]


def get_iso_time() -> datetime:
    cb_client = cbpro.PublicClient()
    while True:
        try:
            server_time = cb_client.get_time()
            return dateutil.parser.parse(server_time['iso'])
        except json.JSONDecodeError:
            continue
