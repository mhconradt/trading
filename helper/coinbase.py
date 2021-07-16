import json
import time
import typing as t
from datetime import datetime
from uuid import uuid4

import cbpro
import dateutil.parser
import requests
from ratelimit import rate_limited, sleep_and_retry

from exceptions import InternalServerError


@sleep_and_retry
@rate_limited(period=1, calls=15)
def wait_for_authenticated_rate_limit() -> None:
    pass


@sleep_and_retry
@rate_limited(period=1, calls=10)
def wait_for_public_rate_limit() -> None:
    pass


# NOTE: There is still no rate limit on paginated messages

class PublicClient(cbpro.PublicClient):
    def get_products(self) -> t.List[dict]:
        wait_for_public_rate_limit()
        return super(PublicClient, self).get_products()

    def get_time(self) -> dict:
        wait_for_public_rate_limit()
        return super(PublicClient, self).get_time()

    def get_product_trades(self, product_id, before='', after='', limit=None,
                           result=None) -> t.Iterator[dict]:
        wait_for_public_rate_limit()
        return super(PublicClient, self).get_product_trades(product_id, before,
                                                            after, limit,
                                                            result)

    def _reset_session(self) -> None:
        self.session = requests.Session()

    def _send_message(self, method, endpoint, params=None, data=None):
        method = method.upper()
        retryable = method == 'GET' or method == 'DELETE'
        while True:
            try:
                url = self.url + endpoint
                r = self.session.request(method, url, params=params, data=data,
                                         auth=self.auth, timeout=30)
                if r.status_code >= 500 and retryable:
                    self._reset_session()
                    time.sleep(1)
                    continue
                elif r.status_code >= 500:
                    raise InternalServerError()
                elif r.status_code == 429:
                    time.sleep(1)
                    continue
                return r.json()
            except requests.RequestException as e:
                self._reset_session()
                time.sleep(1)
                if retryable:
                    continue
                else:
                    raise e  # need to implement retry logic elsewhere


class AuthenticatedClient(PublicClient, cbpro.AuthenticatedClient):
    def get_accounts(self) -> t.List[dict]:
        wait_for_authenticated_rate_limit()
        return super(AuthenticatedClient, self).get_accounts()

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
                                              funds=funds,
                                              client_oid=client_oid,
                                              stp=stp,
                                              overdraft_enabled=False,
                                              funding_amount=funding_amount)

    def cancel_order(self, order_id) -> t.List[str]:
        wait_for_authenticated_rate_limit()
        return super(AuthenticatedClient, self).cancel_order(order_id)

    def cancel_all(self, product_id=None):
        wait_for_authenticated_rate_limit()
        return super(AuthenticatedClient, self).cancel_all(product_id)

    def get_order(self, order_id) -> dict:
        wait_for_authenticated_rate_limit()
        return super(AuthenticatedClient, self).get_order(order_id)

    def get_orders(self, product_id=None, status=None,
                   **kwargs) -> t.Iterator[dict]:
        wait_for_authenticated_rate_limit()
        return super(AuthenticatedClient, self).get_orders(product_id,
                                                           status=status)

    def get_fees(self) -> dict:
        wait_for_authenticated_rate_limit()
        return super(AuthenticatedClient, self)._send_message('GET', '/fees')

    def get_order_by_client_oid(self, client_oid: str) -> t.Optional[dict]:
        wait_for_authenticated_rate_limit()
        url = f'{self.url}/orders/client:{client_oid}'
        response = requests.get(url, auth=self.auth)
        if response.status_code == 200:
            return response.json()
        else:
            return None

    def retryable_market_order(self, *args,
                               **kwargs) -> dict:
        client_oid = kwargs.get('client_oid', str(uuid4()))
        kwargs['client_oid'] = client_oid
        tries = 0
        while True:
            try:
                return self.place_market_order(*args, **kwargs)
            except (requests.RequestException, InternalServerError) as e:
                if order := self.get_order_by_client_oid(client_oid):
                    return order
                else:
                    tries += 1
                    if tries >= kwargs.get('tries', 2):
                        raise e

    def retryable_limit_order(self, *args,
                              **kwargs) -> dict:
        client_oid = kwargs.get('client_oid', str(uuid4()))
        kwargs['client_oid'] = client_oid
        tries = 0
        while True:
            try:
                return self.place_limit_order(*args, **kwargs)
            except (requests.RequestException, InternalServerError) as e:
                if order := self.get_order_by_client_oid(client_oid):
                    return order
                else:
                    tries += 1
                    if tries >= kwargs.get('tries', 2):
                        raise e


public_client = PublicClient()


def get_usd_products() -> t.List[dict]:
    products = public_client.get_products()
    return [product for product in products if
            product['quote_currency'] == 'USD']


def get_usd_product_ids() -> t.List[str]:
    return [product['id'] for product in get_usd_products()]


def get_server_time() -> datetime:
    while True:
        try:
            server_time = public_client.get_time()
            return dateutil.parser.parse(server_time['iso'])
        except json.JSONDecodeError:
            continue
