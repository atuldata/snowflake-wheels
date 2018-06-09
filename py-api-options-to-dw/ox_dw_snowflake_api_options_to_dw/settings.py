"""
Common settings go here.
"""
import os
import yaml

APP_NAME = 'api-options-to-dw'
APP_ROOT = \
    os.environ['APP_ROOT'] if 'APP_ROOT' in os.environ else os.environ['PWD']
LOCK_ROOT = os.path.join(APP_ROOT, 'locks')
CONF_ROOT = os.path.join(APP_ROOT, 'conf')
DEFAULT_CONFIG_FILE = os.path.join(CONF_ROOT, '.'.join([APP_NAME, 'yaml']))
OUTPUT_DIR = os.path.join(APP_ROOT, 'output', APP_NAME)
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
DELIMITER = chr(30)
ENCLOSED = '"'
ESCAPE = '\\'
NULL = 'GhK1'

DEFAULT_PATH = "buckets/ox3-api-options/keys"

LOAD_STATE_TEMPLATE = "last_delivery_%s_load_from_api"


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
DB_NAMESPACE = '.'.join([DB_CONFIG.get('database'), DB_CONFIG.get('schema')])

QUERY_STMT = """
    SELECT %(source_columns)s FROM %(source_table)s"""

PUT_STMT = """
    PUT %(temp_file)s %(stage_location)s"""

COPY_STMT = """
    COPY INTO %(temp_table)s (%(dest_columns)s)
    FROM %(stage_location)s
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '%(delimiter)s')
    ON_ERROR = ABORT_STATEMENT
    PURGE = TRUE"""

CREATE_STMT = """
    CREATE TEMPORARY TABLE IF NOT EXISTS %(temp_table)s
    STAGE_FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '%(delimiter)s')
    AS SELECT %(dest_columns)s FROM %(dest_table)s limit 0"""

INSERT_STMT = """
    INSERT INTO %(dest_table)s(%(dest_columns)s)
    SELECT %(temp_columns)s
    FROM %(temp_table)s temp
    LEFT JOIN %(dest_table)s dest ON %(join_keys)s
    WHERE %(dest_keys)s"""

MERGE_STMT = """
    MERGE INTO %(dest_table)s dest
    USING %(temp_table)s temp
    ON %(join_keys)s
    AND NOT %(join_values)s
    WHEN MATCHED THEN UPDATE
    SET %(set_columns)s"""
