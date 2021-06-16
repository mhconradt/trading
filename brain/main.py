from datetime import timedelta
from decimal import Decimal

from influxdb_client import InfluxDBClient

from cbpro import AuthenticatedClient
from indicators import Ticker, MoonShot
from settings import influx_db as influx_db_settings, \
    coinbase as coinbase_settings
from .portfolio_manager import PortfolioManager
from .stop_loss import SimpleStopLoss
from .volatility_cooldown import VolatilityCoolDown

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
    moonshot = MoonShot(client, EXCHANGE)
    ticker = Ticker(client, EXCHANGE, timedelta(minutes=-1))
    coinbase = AuthenticatedClient(key=coinbase_settings.API_KEY,
                                   b64secret=coinbase_settings.SECRET,
                                   passphrase=coinbase_settings.PASSPHRASE,
                                   api_url=coinbase_settings.API_URL)
    stop_loss = SimpleStopLoss(take_profit_ratio=Decimal('1.013'),
                               stop_loss_ratio=Decimal('0.975'))
    cool_down = VolatilityCoolDown(timedelta(minutes=5))
    manager = PortfolioManager(coinbase, price_indicator=ticker,
                               score_indicator=moonshot, stop_loss=stop_loss,
                               cool_down=cool_down)
    try:
        manager.run()
    finally:
        manager.shutdown()


if __name__ == '__main__':
    main()
