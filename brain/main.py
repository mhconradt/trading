from collections import defaultdict
from datetime import timedelta
from decimal import Decimal
import time
import typing as t

from cbpro import AuthenticatedClient
from influxdb_client import InfluxDBClient
from pandas import Series

from brain.arithmetic import Position
from settings import influx_db as influx_db_settings, \
    coinbase as coinbase_settings
from indicators import Momentum, Ticker

ASSET_LIMIT = 10

EXCHANGE = 'coinbasepro'

ADJUST_INTERVAL = 12

STOP_LOSS_RATIO = Decimal('0.965')

TAKE_PROFIT_RATIO = Decimal('1.01')


# TODO: Position tracking


class SmartPosition:
    def __init__(self, market: str, cost: Decimal, size: Decimal):
        self.market = market
        self.cost = cost
        self.size = size
        self.net_take_profit = TAKE_PROFIT_RATIO
        self.net_stop_loss = STOP_LOSS_RATIO
        self.time = time.monotonic()

    def stop_loss_price(self, fee: Decimal) -> Decimal:
        return self.net_stop_loss * self.buy_price * fee

    def take_profit_price(self, fee: Decimal) -> Decimal:
        return self.net_take_profit * self.buy_price * fee

    @property
    def buy_price(self) -> Decimal:
        return self.cost / self.size

    def adjust_ratios(self, price: Decimal, fee: Decimal) -> None:
        diff = time.monotonic() - self.time
        if diff < ADJUST_INTERVAL:
            return None
        # this is counter-cyclical. price has to increase to increase TP/SL
        if price > self.take_profit_price(fee):
            self.net_take_profit *= TAKE_PROFIT_RATIO
            self.net_stop_loss = self.net_take_profit * STOP_LOSS_RATIO
        self.time = time.monotonic()

    def sell_me(self, price: Decimal, fee: Decimal) -> bool:
        self.adjust_ratios(price, fee)
        return price < self.stop_loss_price(fee)


def liquidate(client: AuthenticatedClient):
    for account in client.get_accounts():
        if account['currency'] == 'USD':
            continue
        product = f'{account["currency"]}-USD'
        if Decimal(account['available']):
            client.place_market_order(product,
                                      side='sell',
                                      size=account['available'])


def get_fees(client: AuthenticatedClient) -> t.Tuple[Decimal, Decimal]:
    fees = getattr(client, '_send_message')('GET', '/fees')
    return Decimal(fees['taker_fee']), Decimal(fees['maker_fee'])


def manage_positions(client: AuthenticatedClient,
                     positions: t.Dict[str, Position],
                     ticker: Series) -> t.Dict[str, Position]:
    next_positions = defaultdict(Position)
    product_infos = {product['id']: product for product in
                     client.get_products()}
    for product, position in positions.items():
        base_increment = Decimal(product_infos[product]['base_increment'])
        quote_increment = Decimal(product_infos[product]['quote_increment'])
        price = Decimal.from_float(ticker[product]).quantize(quote_increment)
        price_change = price / position.price
        max_size = Decimal(product_infos[product]['max_size'])
        size = min(position.size, max_size).quantize(base_increment)
        if not STOP_LOSS_RATIO < price_change < TAKE_PROFIT_RATIO:
            order = client.place_limit_order(
                product_id=product,
                side='sell',
                price=str(price),
                size=str(size),
                time_in_force='IOC',
            )
            filled_size = Decimal(order['filled_size'])
            executed_value = Decimal(order['executed_value'])
            fill_fees = Decimal(order['fill_fees'])
            cost = position.cost - (executed_value - fill_fees)
            next_position = Position(size=position.size - filled_size,
                                     cost=cost)
            if position.size:
                next_positions[product] += next_position
        else:
            next_positions[product] = position
    return next_positions


def buy_stuff(client: AuthenticatedClient, scores: Series, ticker,
              available_funds: Decimal,
              taker_fee: Decimal,
              maker_fee: Decimal) -> t.Dict[str, Position]:
    # what should we buy?
    buys = scores[scores > 0.]
    product_infos = {product['id']: product for product in
                     client.get_products()}
    shopping_list = buys.sort_values(ascending=False).head(ASSET_LIMIT).index
    item_count = len(shopping_list)
    # TODO: add position limits
    positions = dict()
    for product in shopping_list:
        fee_ratio = (taker_fee + Decimal('1')) ** -1
        desired_funds = available_funds / item_count * fee_ratio
        last_known_price = Decimal.from_float(ticker[product])
        base_increment = Decimal(product_infos[product]['base_increment'])
        quote_increment = Decimal(product_infos[product]['quote_increment'])
        price = last_known_price.quantize(quote_increment)
        size = (desired_funds / last_known_price).quantize(base_increment)
        if not size >= Decimal(product_infos[product]['base_min_size']):
            continue
        max_size = Decimal(product_infos[product]['base_max_size'])
        size = min(size, max_size)
        order = client.place_limit_order(product, 'buy', price=str(price),
                                         size=str(size), time_in_force='IOC')
        cost = Decimal(order['executed_value']) + Decimal(order['fill_fees'])
        filled_size = Decimal(order['filled_size'])
        if filled_size:
            positions[product] = Position(size=filled_size, cost=cost)
    return positions


def initialize_positions(client: AuthenticatedClient) -> t.Dict[str, Position]:
    positions = defaultdict(Position)
    return positions


def main() -> None:
    client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                            influx_db_settings.INFLUX_TOKEN,
                            org_id=influx_db_settings.INFLUX_ORG_ID,
                            org=influx_db_settings.INFLUX_ORG)
    momentum_5m = Momentum(client, EXCHANGE, timedelta(minutes=5),
                           start=timedelta(minutes=-30), stop=timedelta(0))
    momentum_15m = Momentum(client, EXCHANGE, timedelta(minutes=15),
                            start=timedelta(minutes=-60), stop=timedelta(0))
    ticker = Ticker(client, EXCHANGE, timedelta(minutes=-1))
    coinbase = AuthenticatedClient(key=coinbase_settings.API_KEY,
                                   b64secret=coinbase_settings.SECRET,
                                   passphrase=coinbase_settings.PASSPHRASE,
                                   api_url=coinbase_settings.API_URL)
    usd_account_id, = [account['id'] for account in coinbase.get_accounts() if
                       account['currency'] == 'USD']
    positions = initialize_positions(coinbase)
    # TODO: Track positions
    # TODO: Handle stale or missing data
    # TODO: Kill switch
    # for making incremental updates to portfolio
    start_time = coinbase.get_time()['iso']
    while True:
        taker_fee, maker_fee = get_fees(coinbase)
        ticks = ticker.compute()
        ticks = ticks[ticks.index.str.endswith('-USD')]
        manage_positions(coinbase, positions, ticks.price)
        available = Decimal(coinbase.get_account(usd_account_id)['available'])
        mom_5 = momentum_5m.compute()
        mom_5 = mom_5[mom_5.columns[mom_5.columns.str.endswith('-USD')]]
        mom_15 = momentum_15m.compute()
        mom_15 = mom_15[mom_15.columns[mom_15.columns.str.endswith('-USD')]]
        scores = score_assets(mom_15, mom_5)
        buy_stuff(coinbase, scores, ticks.price, available, taker_fee,
                  maker_fee)


def score_assets(mom_15: Series, mom_5: Series) -> Series:
    this_mom5 = mom_5.iloc[-1]
    this_mom15 = mom_15.iloc[-1]
    last_mom5 = mom_5.iloc[-2]
    last_mom15 = mom_15.iloc[-2]
    # either this...
    positive = (this_mom5 > 0.) & (last_mom5 > 0.) & (this_mom15 > 0.) & (
            last_mom15 > 0.)
    increasing = (this_mom5 > 0.) & (this_mom15 > 0.)
    accelerating = (this_mom5 > last_mom5) & (this_mom15 > last_mom15)
    buy_mask = positive & increasing & accelerating
    mom5_diff = this_mom5 - last_mom5
    mom15_diff = this_mom15 - last_mom15
    scores = buy_mask * (mom5_diff + mom15_diff)
    return scores


if __name__ == '__main__':
    main()
