"""
Common constants and definitions here.
"""
import os
import sys
import yaml

APP_NAME = 'odfi_etl'
try:
    APP_ROOT = os.environ['APP_ROOT']
except KeyError:
    sys.stderr.write("APP_ROOT is a required in the env! "
                     "This is the base where the application will run.\n")
    sys.exit(1)
LOG_ROOT = os.path.join(APP_ROOT, 'logs')
LOCK_ROOT = os.path.join(APP_ROOT, 'locks')
CONF_ROOT = os.path.join(APP_ROOT, 'conf')
JOB_CONF_ROOT = os.path.join(APP_ROOT, 'jobs', 'odfi_etls')
APP_CONF_ROOT = os.path.join(
    os.path.join(os.path.abspath(os.path.dirname(__file__)), '..'),
    'app_config')
DELIM = ','
COMPRESSION = 'GZIP'
DOWNLOAD_STATUS_TABLE_NAME = 'odfi_etl_download_status'
DOWNLOAD_SORT_KEY = 'serial'
UPLOAD_STATUS_TABLE_NAME = 'odfi_etl_upload_status'
UPLOAD_SORT_KEY = 'readable_interval'
CREATE_STATUS_TABLE = """
CREATE TABLE IF NOT EXISTS %s(
    job_name varchar(100) NOT NULL,
    odfi_feed_name varchar(100) NOT NULL,
    odfi_version int NOT NULL,
    dataset_serial int NOT NULL,
    dataset_readable_interval varchar(13) NOT NULL,
    dataset_start_timestamp timestamp NOT NULL,
    dataset_end_timestamp timestamp NOT NULL,
    dataset_schema_version int NOT NULL,
    dataset_record_count int NOT NULL,
    dataset_data_size int NOT NULL,
    dataset_revision int NOT NULL,
    dataset_date_created timestamp NOT NULL,
    job_start_time timestamp NOT NULL,
    job_end_time timestamp NOT NULL
)"""

INSERT_STATUS_STMT = """
INSERT INTO %s
(job_name, odfi_feed_name, odfi_version, dataset_serial,
 dataset_readable_interval, dataset_start_timestamp, dataset_end_timestamp,
 dataset_schema_version, dataset_record_count, dataset_data_size,
 dataset_revision, dataset_date_created, job_start_time, job_end_time)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

def get_conf(name, base_path=JOB_CONF_ROOT):
    """
    Returns the read in yaml config.
    """
    with open(os.path.join(base_path, '%s.yaml' % name)) as in_yaml:
        return yaml.load(in_yaml)


ENV = get_conf('env', CONF_ROOT)
ODFI_CONF = {
    'ODFI_HOST': ENV.get('ODFI_HOST'),
    'ODFI_USER': ENV.get('ODFI_USER'),
    'ODFI_PASS': ENV.get('ODFI_PASS'),
    'DATA_DIR': os.path.join(APP_ROOT, 'output', 'odfi'),
    'CACHE_META_DATA': True
}
STAGE_NAME = '_'.join([
    ENV['DB_CONNECTIONS']['SNOWFLAKE']['KWARGS'].get('schema', 'test'),
    'odfi_etl_stage'
])
