import environs

env = environs.Env()

# setup for local dev

INFLUX_USER = env.str('INFLUX_USER', 'max')

INFLUX_ORG = env.str('INFLUX_ORG_ID', 'quant')

INFLUX_ORG_ID = env.str('INFLUX_ORG_ID', '310a381375f53c68')

INFLUX_URL = env.str('INFLUX_HOST', 'http://localhost:8086')

INFLUX_TOKEN = env.str('INFLUX_TOKEN')

INFLUX_BUCKET = env.str('INFLUX_BUCKET', 'trades')
