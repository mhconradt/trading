import json
import typing as t
from datetime import datetime

import cbpro
import dateutil.parser
import requests
from retry import retry

public_client = cbpro.PublicClient()


def get_usd_products() -> t.List[dict]:
    products = public_client.get_products()
    return [product for product in products if
            product['quote_currency'] == 'USD']


def get_usd_product_ids() -> t.List[str]:
    return [product['id'] for product in get_usd_products()]


@retry(requests.RequestException, tries=2, delay=15)
def get_server_time() -> datetime:
    while True:
        try:
            server_time = public_client.get_time()
            return dateutil.parser.parse(server_time['iso'])
        except json.JSONDecodeError:
            continue
