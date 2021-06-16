from datetime import timedelta
from decimal import Decimal

from cbpro import AuthenticatedClient
from influxdb_client import InfluxDBClient
from pandas import Series

from settings import influx_db as influx_db_settings, \
    coinbase as coinbase_settings
from indicators import Momentum, Ticker

ASSET_LIMIT = 10

EXCHANGE = 'coinbasepro'

ADJUST_INTERVAL = 12

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
