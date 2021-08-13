from decimal import Decimal

from environs import Env

env = Env()

EXCHANGE = env.str('EXCHANGE', 'coinbasepro')
STOP_LOSS = Decimal(env.str('STOP_LOSS', '0.9'))
TAKE_PROFIT = Decimal(env.str('TAKE_PROFIT', '2'))
QUOTE = Decimal(env.str('QUOTE'))
