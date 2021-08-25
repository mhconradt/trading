from environs import Env

env = Env()

API_KEY = env.str('CB_API_KEY')
SECRET = env.str('CB_SECRET')
PASSPHRASE = env.str('CB_PASSPHRASE')
API_URL = env.str('CB_API_URL', 'https://api.pro.coinbase.com')
