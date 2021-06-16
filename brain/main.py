import logging
from datetime import timedelta

from cbpro import AuthenticatedClient
from influxdb_client import InfluxDBClient

from indicators import Ticker, MoonShot
from settings import influx_db as influx_db_settings, \
    coinbase as coinbase_settings, portfolio as portfolio_settings
from .portfolio_manager import PortfolioManager
from .stop_loss import SimpleStopLoss
from .volatility_cooldown import VolatilityCoolDown

logging.basicConfig(format='%(levelname)s:%(module)s:%(message)s',
                    level=logging.DEBUG)


def main() -> None:
    client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                            influx_db_settings.INFLUX_TOKEN,
                            org_id=influx_db_settings.INFLUX_ORG_ID,
                            org=influx_db_settings.INFLUX_ORG)
    moonshot = MoonShot(client, portfolio_settings.EXCHANGE)
    ticker = Ticker(client, portfolio_settings.EXCHANGE, timedelta(minutes=-1))
    coinbase = AuthenticatedClient(key=coinbase_settings.API_KEY,
                                   b64secret=coinbase_settings.SECRET,
                                   passphrase=coinbase_settings.PASSPHRASE,
                                   api_url=coinbase_settings.API_URL)
    stop_loss = SimpleStopLoss(take_profit=portfolio_settings.TAKE_PROFIT,
                               stop_loss=portfolio_settings.STOP_LOSS)
    cool_down = VolatilityCoolDown(timedelta(minutes=5))
    manager = PortfolioManager(coinbase, price_indicator=ticker,
                               score_indicator=moonshot, stop_loss=stop_loss,
                               cool_down=cool_down,
                               market_blacklist={'USDT-USD'})
    try:
        manager.run()
    finally:
        manager.shutdown()


if __name__ == '__main__':
    main()
