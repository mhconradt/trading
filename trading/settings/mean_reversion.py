from datetime import timedelta

from environs import Env

env = Env()

EMA_PERIODS = env.int("MR_EMA_PERIODS")
BASE_BUY_FRACTION = env.float("MR_BUY_FRACTION")
BASE_SELL_FRACTION = env.float("MR_SELL_FRACTION")

TRADE_BUCKET = 'level1'
TICKER_BUCKET = 'level1'
FREQUENCY = timedelta(minutes=1)

BUY_TARGET_SECONDS = env.int("BUY_TARGET_SECONDS", 300)
SELL_TARGET_SECONDS = env.int("SELL_TARGET_SECONDS", 300)
