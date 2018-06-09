"""
Common settings for this module.
"""
import os
import yaml
from ox_dw_logger import get_etl_logger

APP_NAME = 'ox_salesforce_pull'
APP_ROOT = \
    os.environ['APP_ROOT'] if 'APP_ROOT' in os.environ else os.environ['PWD']
CONF_ROOT = os.path.join(APP_ROOT, 'conf')
LOCK_ROOT = os.path.join(APP_ROOT, 'locks')


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
CONFIG = get_config(APP_NAME)
DB_CONFIG = ENV['DB_CONNECTIONS']['SNOWFLAKE']['KWARGS']
DB_NAME = DB_CONFIG.get('database')
DB_SCHEMA = DB_CONFIG.get('schema')

TMP = os.environ['TMP'] if 'TMP' in os.environ else '/tmp'
TEMP_FILE_DIR = os.path.join(TMP, APP_NAME)
DELIMITER = chr(30)
MAX_ATTEMPTS = 5
WAIT_BETWEEN_ATTEMPTS = 2000  # ms

LOGGER = get_etl_logger(APP_NAME)

GET_COLUMNS = """
    SELECT COLUMN_NAME, ORDINAL_POSITION
    FROM   {0}.INFORMATION_SCHEMA.COLUMNS
    WHERE  TABLE_SCHEMA = ?
      AND  TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION"""

INSERT_MISSING_COLUMNS = """
    INSERT INTO monitor_sf_load
    (table_name, column_name)
    VALUES (?, ?)"""

TRUNCATE_MONITOR_TABLE = """
    TRUNCATE TABLE monitor_sf_load"""
