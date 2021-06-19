import json
import typing as t
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

import cbpro
import dateutil.parser
import requests
from retry import retry

public_client = cbpro.PublicClient()


@retry(requests.RequestException, tries=3, delay=15)
def get_usd_products() -> t.List[dict]:
    products = public_client.get_products()
    return [product for product in products if
            product['quote_currency'] == 'USD']


def get_usd_product_ids() -> t.List[str]:
    return [product['id'] for product in get_usd_products()]


@retry(requests.RequestException, tries=3, delay=15)
def get_server_time() -> datetime:
    while True:
        try:
            server_time = public_client.get_time()
            return dateutil.parser.parse(server_time['iso'])
        except json.JSONDecodeError:
            continue


@retry(requests.RequestException, tries=3, delay=15)
def get_order_by_client_oid(client: cbpro.AuthenticatedClient,
                            client_oid: str) -> t.Optional[dict]:
    url = f'{client.url}/orders/client:{client_oid}'
    response = requests.get(url, auth=client.auth)
    if response.status_code == 200:
        return response.json()
    else:
        return None


@retry(requests.RequestException, tries=3, delay=15)
def get_last_trade_price(product_id: str) -> Decimal:
    ticker = public_client.get_product_ticker(product_id)
    return ticker['price']


@retry(requests.RequestException, tries=3, delay=15)
def place_limit_order(client: cbpro.AuthenticatedClient, *args,
                      **kwargs) -> dict:
    client_oid = kwargs.get('client_oid', str(uuid4()))
    kwargs['client_oid'] = client_oid
    try:
        return client.place_limit_order(*args, **kwargs)
    except requests.RequestException as e:
        if order := get_order_by_client_oid(client, client_oid):
            return order
        else:
            raise e


@retry(requests.RequestException, tries=3, delay=15)
def place_market_order(client: cbpro.AuthenticatedClient, *args,
                       **kwargs) -> dict:
    client_oid = kwargs.get('client_oid', str(uuid4()))
    kwargs['client_oid'] = client_oid
    try:
        return client.place_market_order(*args, **kwargs)
    except requests.RequestException as e:
        if order := get_order_by_client_oid(client, client_oid):
            return order
        else:
            raise e
