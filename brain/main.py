from datetime import timedelta
from decimal import Decimal
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

STOP_LOSS_RATIO = Decimal('0.965')

TAKE_PROFIT_RATIO = Decimal('1.01')


def liquidate(client: AuthenticatedClient):
    for account in client.get_accounts():
        if account['currency'] == 'USD':
            continue
        product = f'{account["currency"]}-USD'
        if Decimal(account['available']):
            client.place_market_order(product,
                                      side='sell',
                                      size=account['available'])


def manage_positions(client: AuthenticatedClient,
                     positions: t.List[Position],
                     ticker: Series) -> t.List[Position]:
    next_positions = []
    product_infos = {product['id']: product for product in
                     client.get_products()}
    for position in positions:
        product = position.market
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
            position = Position(market=position.market,
                                size=position.size - filled_size,
                                cost=cost)
            if position.size:
                next_positions.append(position)
        else:
            next_positions.append(position)
    return next_positions


def buy_stuff(client: AuthenticatedClient, scores: Series, ticker,
              available_funds: Decimal) -> t.List[Position]:
    # what should we buy?
    buys = scores[scores > 0.]
    product_infos = {product['id']: product for product in
                     client.get_products()}
    shopping_list = buys.sort_values(ascending=False).head(ASSET_LIMIT).index
    item_count = len(shopping_list)
    # add position limits
    positions = []
    for product in shopping_list:
        desired_funds = available_funds / item_count
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
        # I thought this would fulfill the order
        print(order)
        cost = Decimal(order['executed_value']) + Decimal(order['fill_fees'])
        filled_size = Decimal(order['filled_size'])
        if filled_size:
            pos = Position(market=product, size=filled_size, cost=cost)
            positions.append(pos)
    return positions


def merge_positions(old: t.List[Position],
                    new: t.List[Position]) -> t.List[Position]:
    old = {position.market: position for position in old}
    new = {position.market: position for position in new}
    markets = set(old) | set(new)
    positions = []
    for market in markets:
        position = Position.zero(market)
        if market in old:
            position += old[market]
        if market in new:
            position += new[market]
        positions.append(position)
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
    positions = []
    while True:
        ticks = ticker.compute()
        ticks = ticks[ticks.index.str.endswith('-USD')]
        # TODO: Handle stale / missing data?
        positions = manage_positions(coinbase, positions, ticks.price)
        available = Decimal(coinbase.get_account(usd_account_id)['available'])
        mom_5 = momentum_5m.compute()
        mom_5 = mom_5[mom_5.columns[mom_5.columns.str.endswith('-USD')]]
        mom_15 = momentum_15m.compute()
        mom_15 = mom_15[mom_15.columns[mom_15.columns.str.endswith('-USD')]]
        scores = score_assets(mom_15, mom_5)
        purchases = buy_stuff(coinbase, scores, ticks.price, available)
        positions = merge_positions(positions, purchases)


def score_assets(mom_15: Series, mom_5: Series) -> Series:
    this_mom5 = mom_5.iloc[-1]
    this_mom15 = mom_15.iloc[-1]
    last_mom5 = mom_5.iloc[-2]
    last_mom15 = mom_15.iloc[-2]
    increasing = (this_mom5 > 0.) & (this_mom15 > 0.)
    accelerating = (this_mom5 > last_mom5) & (this_mom15 > last_mom15)
    buy_mask = increasing & accelerating
    mom5_diff = this_mom5 - last_mom5
    mom15_diff = this_mom15 - last_mom15
    scores = buy_mask * (mom5_diff + mom15_diff)
    return scores


if __name__ == '__main__':
    main()
