from datetime import timedelta

from environs import Env

env = Env()

EMA_PERIODS = env.int("MR_EMA_PERIODS")
BASE_BUY_FRACTION = env.float("MR_BUY_FRACTION")
BASE_SELL_FRACTION = env.float("MR_SELL_FRACTION")

TRADE_BUCKET = 'level1'
TICKER_BUCKET = 'level1'
FREQUENCY = timedelta(minutes=1)

BUY_HORIZON = env.timedelta("BUY_TARGET_SECONDS", timedelta(minutes=5),
                            precision='seconds')
SELL_HORIZON = env.timedelta("SELL_TARGET_SECONDS", timedelta(minutes=5),
                             precision='seconds')
RMMI_PERIOD = env.timedelta("RMMI_SECONDS", timedelta(minutes=5),
                            precision='seconds')
