"""
Common settings for the rabbitmq client, extractor and loader.
"""
import csv
import os
import yaml

APP_NAME = 'api-extractor'
HARMONIZER_NAME = 'api-harmonizer'
LOADER_APP_NAME = 'api-loader'

APP_ROOT = \
    os.environ['APP_ROOT'] if 'APP_ROOT' in os.environ else os.environ['PWD']
CONF_ROOT = os.path.join(APP_ROOT, 'conf')
LOCK_ROOT = os.path.join(APP_ROOT, 'locks')
LOG_DIRECTORY = os.path.join(APP_ROOT, 'logs')
LOG_DIR = os.path.join(LOG_DIRECTORY, APP_NAME)
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
OUTPUT_DIR = os.path.join(APP_ROOT, 'output')
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


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


CONFIG = get_config(APP_NAME)
FILE_FORMAT = CONFIG.get('FILE_FORMAT', 'csv')
FLUSH_PERIOD = int(CONFIG.get('FLUSH_PERIOD', 5))
MAX_LOADERS = int(CONFIG.get('MAX_LOADERS', 5))

ENV = get_config('env')
DB_CONFIG = ENV['DB_CONNECTIONS']['SNOWFLAKE']['KWARGS']
DB_NAMESPACE = '.'.join([DB_CONFIG.get('database'), DB_CONFIG.get('schema')])

LOCAL_DB = os.path.join(OUTPUT_DIR, '.'.join([APP_NAME, 'db']))
DEFAULT_SCHEMA_FILE = \
    os.path.join(CONF_ROOT, 'api-extractor-schema-config.yaml')
DELIMITER = '\x1e'
ESCAPE_CHAR = '\"\\"'
NULL = 'None'
RAW_TABLE_PREFIX = 'api_raw'
DESTINATIONS = '_destinations'
CSV_KWARGS = {
    'quoting': csv.QUOTE_NONE,
    'delimiter': DELIMITER,
    'doublequote': False,
    'quotechar': '',
    'escapechar': ''
}

CREATE_LOCAL_TEMP_STMT = """
CREATE TEMPORARY TABLE IF NOT EXISTS %s(
    %s,
    updated_date_sid int
)
STAGE_FILE_FORMAT = (TYPE = CSV
                     FIELD_DELIMITER = {0!r}
                     ESCAPE_UNENCLOSED_FIELD = {1}
                     NULL_IF = {2!r}
                     ERROR_ON_COLUMN_COUNT_MISMATCH = false)""".format(
    DELIMITER, ESCAPE_CHAR, NULL)

PUT_STATEMENT = """ PUT %(output_file)s %(stage_location)s"""

LOAD_API_RAW_STMT = """
COPY INTO %(table_name)s (%(columns)s, updated_date_sid)
FROM %(stage_location)s
ON_ERROR = ABORT_STATEMENT
ENFORCE_LENGTH = FALSE
PURGE = TRUE"""
