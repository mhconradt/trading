import logging
import sys
from datetime import timedelta

from cbpro import AuthenticatedClient
from influxdb_client import InfluxDBClient

from brain.portfolio_manager import PortfolioManager
from brain.stop_loss import SimpleStopLoss
from brain.volatility_cooldown import VolatilityCoolDown
from indicators import Ticker, PessimisticMoonShot
from settings import influx_db as influx_db_settings, \
    coinbase as coinbase_settings, portfolio as portfolio_settings

logging.basicConfig(format='%(levelname)s:%(module)s:%(message)s',
                    level=logging.DEBUG)


def main() -> None:
    client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                            influx_db_settings.INFLUX_TOKEN,
                            org_id=influx_db_settings.INFLUX_ORG_ID,
                            org=influx_db_settings.INFLUX_ORG)
    moonshot = PessimisticMoonShot(client, portfolio_settings.EXCHANGE,
                                   max_lag=timedelta(seconds=15),
                                   downturn_window=timedelta(hours=6))
    ticker = Ticker(client, portfolio_settings.EXCHANGE,
                    start=timedelta(minutes=-5))
    coinbase = AuthenticatedClient(key=coinbase_settings.API_KEY,
                                   b64secret=coinbase_settings.SECRET,
                                   passphrase=coinbase_settings.PASSPHRASE,
                                   api_url=coinbase_settings.API_URL)
    stop_loss = SimpleStopLoss(take_profit=portfolio_settings.TAKE_PROFIT,
                               stop_loss=portfolio_settings.STOP_LOSS)
    cool_down = VolatilityCoolDown(period=timedelta(minutes=5))
    manager = PortfolioManager(coinbase, price_indicator=ticker,
                               score_indicator=moonshot, stop_loss=stop_loss,
                               cool_down=cool_down,
                               market_blacklist={'USDT-USD', 'DAI-USD'})
    try:
        manager.run()
    finally:
        manager.shutdown()
    sys.exit(1)


if __name__ == '__main__':
    main()