"""
Constants set here.
"""
import os
import yaml

APP_NAME = 'content_topic_loader'
APP_ROOT = \
    os.environ['APP_ROOT'] if 'APP_ROOT' in os.environ else os.environ['PWD']
CONF_ROOT = os.path.join(APP_ROOT, 'conf')
LOCK_ROOT = os.path.join(APP_ROOT, 'locks')
OUTPUT_DIR = os.path.join(APP_ROOT, 'output')
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
LOAD_STATE_NAME = 'last_content_topic_load_from_api'
DELIMITER = chr(30)
STAGING_TABLE_NAME = '_'.join(['stage', APP_NAME])
DEST_TABLE_NAME = 'content_topic_dim'
CONTENT_TOPICS_JSON = 'content_topics.json'

STATEMENTS = [
    """
CREATE TEMPORARY TABLE IF NOT EXISTS %(stage_table_name)s (
    platform_id VARCHAR(40) NOT NULL,
    content_topic_nk INT NOT NULL,
    content_topic_name VARCHAR(255) NOT NULL
    )
STAGE_FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '%(delimiter)s')""", """
PUT %(load_file)s
%(stage_location)s""", """
COPY INTO %(stage_table_name)s
(platform_id, content_topic_nk, content_topic_name)
ON_ERROR = ABORT_STATEMENT
PURGE = TRUE""", """
INSERT INTO %(dest_name)s
(platform_id, content_topic_nk, content_topic_name)
SELECT platform_id, content_topic_nk, content_topic_name
FROM %(stage_table_name)s
WHERE (platform_id, content_topic_nk)
       NOT IN (SELECT platform_id, content_topic_nk
               FROM %(dest_name)s )""", """
MERGE INTO %(dest_name)s c
USING %(stage_table_name)s t
   ON t.platform_id=c.platform_id
  AND t.content_topic_nk=c.content_topic_nk
  AND t.content_topic_name != c.content_topic_name
WHEN MATCHED THEN UPDATE SET content_topic_name = t.content_topic_name"""
]


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
CUSTOMERS_DIR = ENV.get('CUSTOMERS_DIR', '/etc/ox/customers')
DB_CONFIG = ENV['DB_CONNECTIONS']['SNOWFLAKE']['KWARGS']
DB_NAMESPACE = '.'.join([DB_CONFIG.get('database'), DB_CONFIG.get('schema')])
STAGING_LOCATION = '@' + '.%'.join([DB_NAMESPACE, STAGING_TABLE_NAME])
