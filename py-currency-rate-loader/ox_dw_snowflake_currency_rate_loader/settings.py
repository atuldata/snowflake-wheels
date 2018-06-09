"""
Constants set here.
"""
import os
import yaml
from datetime import datetime

APP_NAME = 'currency_rate_loader'
APP_ROOT = \
    os.environ['APP_ROOT'] if 'APP_ROOT' in os.environ else os.environ['PWD']
CONF_ROOT = os.path.join(APP_ROOT, 'conf')
OUTPUT_DIR = os.path.join(APP_ROOT, 'output')
LOCK_ROOT = os.path.join(APP_ROOT, 'locks')
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
LOAD_STATE_NAME = 'last_datetime_currency_exchange_rates_loaded'
MAX_ATTEMPTS = 5
WAIT_BETWEEN_ATTEMPTS = 2000  # ms
END_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


def get_config(name, path=None):
    """
    Reads in the yaml config from the CONF_ROOT
    path: Is the relative path from CONF_ROOT
    name: Is the name of the file without the extension.
    Currently all conf are yaml.
    rtype: dict
    """
    conf_dir = CONF_ROOT
    if path is not None:
        conf_dir = os.path.join(conf_dir, path)
    config_file = os.path.join(conf_dir, '.'.join([name, 'yaml']))
    if os.path.exists(config_file):
        with open(config_file) as conf:
            return yaml.load(conf)

    # If the config_file doesn't exist just return an emtpy dict aas a dict
    # response is expected.
    return {}


ENV = get_config('env')

BASE_CURRENCY = 'USD'
BASE_URL = \
    'http://%s:%s/%s/' % (
        ENV.get('CURRENCY_WS_SERVER'),
        ENV.get('CURRENCY_WS_PORT'),
        ENV.get('CURRENCY_WS_NAME'))

STATEMENTS = ["""
CREATE LOCAL TEMPORARY TABLE tmp_currency_rate_table (
    base VARCHAR(12),
    quote VARCHAR(12),
    date DATE,
    bid FLOAT,
    ask FLOAT,
    last_oanda_updated DATE)""", """
PUT file://{0} @%tmp_currency_rate_table""", """
COPY INTO tmp_currency_rate_table
    (base, quote, date, bid, ask, last_oanda_updated)
    FILE_FORMAT = (
        TYPE = CSV
        )
ON_ERROR = ABORT_STATEMENT
PURGE = TRUE""", """
INSERT INTO currency_exchange_daily_fact
(base, currency, date, exchange_rate, last_oanda_updated, date_sid) 
SELECT base, quote, date, bid, last_oanda_updated, to_number(to_char(date, 'YYYYMMDD')) 
FROM tmp_currency_rate_table 
WHERE (base, quote, date) NOT IN (
         SELECT base, currency, date 
           FROM currency_exchange_daily_fact)"""]
