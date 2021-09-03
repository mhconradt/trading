import json
import logging
import time
import typing as t
from datetime import datetime
from uuid import uuid4

import cbpro
import dateutil.parser
import requests
from ratelimit import rate_limited, sleep_and_retry

from trading.exceptions import InternalServerError

logger = logging.getLogger(__name__)


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
        status_code = response.status_code
        # assumes you're eventually going to get either a 200 or 400
        if status_code == 200:
            return response.json()
        elif status_code == 404:
            return None
        else:  # neanderthal retry
            time.sleep(1)
            self._reset_session()
            logger.debug(f"Retrying status {status_code} for {client_oid}")
            return self.get_order_by_client_oid(client_oid)

    def retryable_market_order(self, *args,
                               **kwargs) -> dict:
        tries = 0
        while True:
            client_oid = str(uuid4())
            kwargs['client_oid'] = client_oid
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
        tries = 0
        while True:
            client_oid = str(uuid4())
            kwargs['client_oid'] = client_oid
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


def get_server_time() -> datetime:
    while True:
        try:
            server_time = public_client.get_time()
            return dateutil.parser.parse(server_time['iso'])
        # if (milliseconds % seconds) == 0 the API returns invalid JSON
        except json.JSONDecodeError:
            continue
