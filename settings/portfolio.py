from decimal import Decimal

from environs import Env

env = Env()

EXCHANGE = env.str('EXCHANGE', 'coinbasepro')
STOP_LOSS = Decimal(env.str('STOP_LOSS', '0.99'))
QUOTE = env.str('QUOTE', 'USD')
MIN_TICK_TIME = env.float('MIN_TICK_TIME', 0.)
