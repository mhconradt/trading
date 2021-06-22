import json
import typing as t
from datetime import datetime
from uuid import uuid4

import cbpro
import dateutil.parser
import requests
from ratelimit import rate_limited, sleep_and_retry
from retry import retry


@sleep_and_retry
@rate_limited(period=1, calls=15)
def wait_for_authenticated_rate_limit() -> None:
    pass


@sleep_and_retry
@rate_limited(period=1, calls=10)
def wait_for_public_rate_limit() -> None:
    pass


class PublicClient(cbpro.PublicClient):
    @retry(requests.RequestException, tries=3, delay=15)
    def get_products(self) -> t.List[dict]:
        wait_for_public_rate_limit()
        return super(PublicClient, self).get_products()

    @retry(requests.RequestException, tries=3, delay=15)
    def get_time(self) -> dict:
        wait_for_public_rate_limit()
        return super(PublicClient, self).get_time()

    def get_product_trades(self, product_id, before='', after='', limit=None,
                           result=None) -> t.Iterator[dict]:
        wait_for_public_rate_limit()
        return super(PublicClient, self).get_product_trades(product_id, before,
                                                            after, limit,
                                                            result)


# TODO: Retry paginated message

class AuthenticatedClient(PublicClient, cbpro.AuthenticatedClient):
    @retry(requests.RequestException, tries=3, delay=15)
    def get_accounts(self) -> t.List[dict]:
        wait_for_authenticated_rate_limit()
        return super(AuthenticatedClient, self).get_accounts()

    @retry(requests.RequestException, tries=3, delay=15)
    def get_account(self, account_id) -> dict:
        wait_for_authenticated_rate_limit()
        return super(AuthenticatedClient, self).get_account(account_id)

    def place_limit_order(self, product_id, side, price, size,
                          client_oid=None,
                          stp=None,
                          time_in_force=None,
                          cancel_after=None,
                          post_only=None,
                          overdraft_enabled=None,
                          funding_amount=None) -> dict:
        wait_for_authenticated_rate_limit()
        return super(AuthenticatedClient,
                     self).place_limit_order(product_id,
                                             side=side,
                                             price=price,
                                             size=size,
                                             client_oid=client_oid,
                                             stp=stp,
                                             time_in_force=time_in_force,
                                             cancel_after=cancel_after,
                                             post_only=post_only,
                                             overdraft_enabled=False,
                                             funding_amount=funding_amount)

    def place_market_order(self, product_id, side, size=None, funds=None,
                           client_oid=None,
                           stp=None,
                           overdraft_enabled=None,
                           funding_amount=None) -> dict:
        wait_for_authenticated_rate_limit()
        return super(AuthenticatedClient,
                     self).place_market_order(product_id,
                                              side=side,
                                              size=size,
                                              client_oid=client_oid,
                                              stp=stp,
                                              overdraft_enabled=False,
                                              funding_amount=funding_amount)

    @retry(requests.RequestException, tries=3, delay=15)
    def cancel_order(self, order_id) -> t.List[str]:
        wait_for_authenticated_rate_limit()
        return super(AuthenticatedClient, self).cancel_order(order_id)

    @retry(requests.RequestException, tries=3, delay=15)
    def cancel_all(self, product_id=None):
        wait_for_authenticated_rate_limit()
        return super(AuthenticatedClient, self).cancel_all(product_id)

    @retry(requests.RequestException, tries=3, delay=15)
    def get_order(self, order_id) -> dict:
        wait_for_authenticated_rate_limit()
        return super(AuthenticatedClient, self).get_order(order_id)

    @retry(requests.RequestException, tries=3, delay=15)
    def get_orders(self, product_id=None, status=None,
                   **kwargs) -> t.Iterator[dict]:
        wait_for_authenticated_rate_limit()
        return super(AuthenticatedClient, self).get_orders(product_id,
                                                           status=status)

    @retry(requests.RequestException, tries=3, delay=15)
    def get_fees(self) -> dict:
        wait_for_authenticated_rate_limit()
        return super(AuthenticatedClient, self)._send_message('GET', '/fees')

    @retry(requests.RequestException, tries=3, delay=15)
    def get_order_by_client_oid(self, client_oid: str) -> t.Optional[dict]:
        wait_for_authenticated_rate_limit()
        url = f'{self.url}/orders/client:{client_oid}'
        response = requests.get(url, auth=self.auth)
        if response.status_code == 200:
            return response.json()
        else:
            return None

    @retry(requests.RequestException, tries=3, delay=15)
    def retryable_market_order(self, *args,
                               **kwargs) -> dict:
        client_oid = kwargs.get('client_oid', str(uuid4()))
        kwargs['client_oid'] = client_oid
        try:
            return self.place_market_order(*args, **kwargs)
        except requests.RequestException as e:
            if order := self.get_order_by_client_oid(client_oid):
                return order
            else:
                raise e

    @retry(requests.RequestException, tries=3, delay=15)
    def retryable_limit_order(self, *args,
                              **kwargs) -> dict:
        client_oid = kwargs.get('client_oid', str(uuid4()))
        kwargs['client_oid'] = client_oid
        try:
            return self.place_limit_order(*args, **kwargs)
        except requests.RequestException as e:
            if order := self.get_order_by_client_oid(client_oid):
                return order
            else:
                raise e


public_client = PublicClient()


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
