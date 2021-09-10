from datetime import timedelta
from decimal import Decimal

from environs import Env

env = Env()

EXCHANGE = env.str('EXCHANGE', 'coinbasepro')
STOP_LOSS = Decimal(env.str('STOP_LOSS', '0.99'))
QUOTE = env.str('QUOTE', 'USD')
MIN_TICK_TIME = env.float('MIN_TICK_TIME', 0.)
STOP_LOSS_COOLDOWN = env.timedelta('COOLDOWN_SECONDS', timedelta(hours=1),
                                   precision='seconds')
CONCENTRATION_LIMIT = env.float('CONCENTRATION_LIMIT', 0.2)
PROBABILISTIC_BUYING = env.bool('PROBABILISTIC_BUYING', False)
