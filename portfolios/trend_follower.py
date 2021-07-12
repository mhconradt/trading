import logging
import signal
import sys
from datetime import timedelta

from influxdb_client import InfluxDBClient

from brain.portfolio_manager import PortfolioManager
from brain.stop_loss import SimpleStopLoss
from brain.volatility_cooldown import VolatilityCoolDown
from helper.coinbase import AuthenticatedClient
from indicators import Ticker, TrailingVolume, TrendFollower
from indicators.fib_trader import FibTrader
from settings import influx_db as influx_db_settings, \
    coinbase as coinbase_settings, portfolio as portfolio_settings

B = 2

A = 3

logging.basicConfig(format='%(levelname)s:%(module)s:%(message)s',
                    level=logging.DEBUG)


def main() -> None:
    client = InfluxDBClient(influx_db_settings.INFLUX_URL,
                            influx_db_settings.INFLUX_TOKEN,
                            org_id=influx_db_settings.INFLUX_ORG_ID,
                            org=influx_db_settings.INFLUX_ORG)
    fibonacci = FibTrader(A, B)
    volume = TrailingVolume(periods=5)
    ticker = Ticker()
    coinbase = AuthenticatedClient(key=coinbase_settings.API_KEY,
                                   b64secret=coinbase_settings.SECRET,
                                   passphrase=coinbase_settings.PASSPHRASE,
                                   api_url=coinbase_settings.API_URL)
    stop_loss = SimpleStopLoss(take_profit=portfolio_settings.TAKE_PROFIT,
                               stop_loss=portfolio_settings.STOP_LOSS)
    cool_down = VolatilityCoolDown(period=timedelta(minutes=0))
    trend_follower = TrendFollower(a=A, b=B)
    manager = PortfolioManager(coinbase, price_indicator=ticker,
                               volume_indicator=volume,
                               score_indicator=fibonacci,
                               trend_indicator=trend_follower,
                               market_blacklist={'USDT-USD', 'DAI-USD'},
                               stop_loss=stop_loss, liquidate_on_shutdown=True,
                               buy_order_type='market',
                               sell_order_type='market',
                               cool_down=cool_down)
    signal.signal(signal.SIGTERM, lambda _, __: manager.shutdown())
    try:
        manager.run()
    finally:
        manager.shutdown()
    sys.exit(1)


if __name__ == '__main__':
    main()
