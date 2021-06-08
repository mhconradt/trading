import typing as t

import cbpro


def get_usd_products() -> t.List[dict]:
    cb_client = cbpro.PublicClient()
    products = cb_client.get_products()
    return [product for product in products if
            product['quote_currency'] == 'USD']


def get_usd_product_ids() -> t.List[str]:
    return [product['id'] for product in get_usd_products()]
