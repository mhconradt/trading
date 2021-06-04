import typing as t

import cbpro


def get_usd_products() -> t.List[str]:
    cb_client = cbpro.PublicClient()
    products = cb_client.get_products()
    usd_markets = [product['id'] for product in products if
                   product['quote_currency'] == 'USD']
    return usd_markets