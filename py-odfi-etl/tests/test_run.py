import os
from shutil import copyfile,  rmtree
import sys
try:
    from io import StringIO
except ImportError:
    from cStringIO import StringIO
from collections import namedtuple
import unittest
import yaml
#SYS_CONF_ROOT = os.path.join(os.environ['APP_ROOT'], 'conf')
#os.environ['APP_ROOT'] = os.path.join(
#    os.path.abspath(os.path.join(os.path.dirname(__file__))))
#from ox_dw_snowflake_odfi_etl.common.settings import ENV
#with open(os.path.join(SYS_CONF_ROOT, 'env.yaml')) as in_env:
#    ENV = yaml.load(in_env)
#ENV['SNOWFLAKE']['schema'] = 'test_odfi_etl'
#from ox_dw_snowflake_odfi_etl.common.settings import JOB_CONF_ROOT, ENV

# Need to override the schema
import ox_dw_snowflake_odfi_etl.common.settings as settings
settings.ENV['SNOWFLAKE']['schema'] = 'test_odfi_etl1'

from ox_dw_logger import get_etl_logger
from ox_dw_snowflake_odfi_etl.common.settings import JOB_CONF_ROOT
from ox_dw_snowflake_odfi_etl import (add_new, download, upload, APP_ROOT, ENV, get_odfi_etl_status, Job, Downloader, Uploader, CREATE, get_dbh, get_conf)

# Let's make sure we are not going to create tables or drop needed schemas.
if not ENV['SNOWFLAKE']['schema'].startswith('test'):
    raise ValueError("The ENV['SNOWFLAKE']['schema'] must start with test!!!")


TEST_JOB_CONF_ROOT = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'jobs', 'odfi_etls')
LOG_FILE = os.path.join(APP_ROOT, 'logs',
                        'odfi_etl_test_job_one.log')  # TODO test_job_one.log
JOB_NAME = 'test_job_one'
OPTIONS = namedtuple('options',
                     ["job_name", "readable_interval_str", "serial", "debug"])
OUTPUT_DIRS = [
    os.path.join(APP_ROOT, 'output', 'odfi', feed)
    for feed in ['TrafficReport', 'RTBReport']
]
MAX_DATASETS = 4
CREATE_SCHEMA_STMT = "CREATE OR REPLACE SCHEMA %s" % ENV['SNOWFLAKE']['schema']
CREATE_TABLE_STMT = """
CREATE TABLE %s.%%s
LIKE mstr_datamart.%%s""" % ENV['SNOWFLAKE']['schema']
DROP_SCHEMA_STMT = "DROP SCHEMA IF EXISTS %s CASCADE" % ENV['SNOWFLAKE']['schema']
TABLES = """
    content_topic_group_bridge
    data_change
    dim_duplicates
    rollup_state
""".split()
CREATES = [
    CREATE,
    """
CREATE TABLE %s.content_topic_group_bridge
(
    content_topic_group_sid int not null,
    content_topic_sid int not null
)""" % ENV['SNOWFLAKE']['schema'],
    """
CREATE SEQUENCE %s.seq_data_change_id""" % ENV['SNOWFLAKE']['schema'],
    """
CREATE TABLE %s.data_change
(
    data_change_id int not null default %s.seq_data_change_id.nextval,
    table_name varchar(256),
    utc_start_datetime timestamp,
    utc_end_datetime timestamp,
    process_name varchar(256),
    utc_modified_datetime timestamp
)""" % (ENV['SNOWFLAKE']['schema'], ENV['SNOWFLAKE']['schema']),
    """
CREATE TABLE %s.dim_duplicates(
    dim_name varchar(256) NOT NULL,
    attr_1 varchar(256) NOT NULL,
    attr_2 varchar(256),
    dup_count int NOT NULL,
    created_datetime timestamp DEFAULT CONVERT_TIMEZONE('UTC', '2015-11-10 01:04:00.121687+00')::TIMESTAMP_NTZ NOT NULL
)""" % ENV['SNOWFLAKE']['schema'],
    """
CREATE TABLE %s.rollup_state
( 
 rs_utc_hour timestamp NOT NULL,
 new_input_data boolean NOT NULL DEFAULT false,
 has_oxts boolean NOT NULL DEFAULT false,
 has_conv boolean NOT NULL DEFAULT false,
 num_runs int,
 needs_rs_run boolean NOT NULL DEFAULT false,
 needs_daily_run boolean NOT NULL DEFAULT false,
 needs_monthly_run boolean NOT NULL DEFAULT false,
 day_start timestamp,
 day_end timestamp,
 modified_datetime timestamp NOT NULL DEFAULT CONVERT_TIMEZONE('UTC',CURRENT_TIMESTAMP)::TIMESTAMP_NTZ,
 new_rollup_data boolean DEFAULT false,
 new_monthly_rollup_data boolean DEFAULT false,
 republishing_data boolean DEFAULT false
)""" % ENV['SNOWFLAKE']['schema'],
    """
CREATE SEQUENCE %s.content_topic_group_sid_seq""" % ENV['SNOWFLAKE']['schema'],
    """
CREATE SEQUENCE %s.sdgh_rowid_seq""" % ENV['SNOWFLAKE']['schema'],
    """
CREATE TABLE %s.content_topic_dim
(
    content_topic_id int IDENTITY,
    platform_id varchar(40) NOT NULL,
    content_topic_nk int NOT NULL,
    content_topic_name varchar(255)
)""" % ENV['SNOWFLAKE']['schema'],
    """
CREATE TABLE %s.content_topic_group_dim
(
    content_topic_group_sid int NOT NULL DEFAULT %s.content_topic_group_sid_seq.nextval,
    ad_unit_nk int,
    platform_id varchar(40) NOT NULL,
    content_topic_group_id_string varchar(512) NOT NULL
)""" % (ENV['SNOWFLAKE']['schema'], ENV['SNOWFLAKE']['schema']),
    """
CREATE TABLE %s.supply_demand_geo_hourly_fact
(
    rowid int NOT NULL DEFAULT %s.sdgh_rowid_seq.nextval,
    utc_date_sid int not null,
    utc_hour_sid int not null,
    publisher_account_nk int not null,
    ad_unit_nk int not null,
    tot_requests int not null default 0,
    tot_view_conversions int not null default 0,
    tot_click_conversions int not null default 0
)""" % (ENV['SNOWFLAKE']['schema'], ENV['SNOWFLAKE']['schema'])
]


class Capturing(list):
    def __enter__(self):
        if sys.version_info >= (2, 7):
            self._stderr = sys.stderr
            sys.stderr = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        if sys.version_info >= (2, 7):
            self.extend(self._stringio.getvalue().splitlines())
            del self._stringio  # free up some memory
            sys.stderr = self._stderr


def cleanup():
    for to_be_removed in [LOG_FILE] + OUTPUT_DIRS:
        if os.path.isfile(to_be_removed):
            os.remove(to_be_removed)
        elif os.path.isdir(to_be_removed):
            rmtree(to_be_removed)
    with get_dbh() as dbh:
        dbh.cursor().execute(DROP_SCHEMA_STMT)


def setup_schema():
    for yaml_file in os.listdir(TEST_JOB_CONF_ROOT):
        copyfile(os.path.join(TEST_JOB_CONF_ROOT, yaml_file), os.path.join(JOB_CONF_ROOT, yaml_file))
    with get_dbh() as dbh:
        dbh.cursor().execute(CREATE_SCHEMA_STMT)
        for stmt in CREATES:
            dbh.cursor().execute(stmt)


class TestETLStatus(unittest.TestCase):
    def setUp(self):
        setup_schema()

    def tearDown(self):
        cleanup()

    def test_init_statement(self):
        output = []
        with Capturing(output) as output:
            with get_dbh() as dbh:
                get_odfi_etl_status(dbh, JOB_NAME, 'some_feed_name', 12123)


class TestLoader(unittest.TestCase):
    def setUp(self):
        setup_schema()
        self.logger = get_etl_logger(JOB_NAME)  # , log_directory=None)
        self.options = OPTIONS(JOB_NAME, '2018-03-14_04', 1181, True)

    def tearDown(self):
        pass
        #cleanup()

    def test_republish(self):
        output = []
        with Capturing(output) as output:
            with get_dbh() as dbh:
                add_new(self.options, dbh)
                loader = Uploader(Job(JOB_NAME, dbh), self.logger)
                for dataset in loader.job.feed.get_datasets_since_serial(
                        self.options.serial, max_datasets=MAX_DATASETS):
                    loader.is_downloaded(dataset)
                    print("\n", dataset.serial,
                          loader.is_republish(dataset), "==", False,
                          loader.job.readable_interval, dataset.readable_interval)
                     
                    self.assertFalse(loader.is_republish(dataset))
                Uploader(Job(JOB_NAME, dbh), self.logger)(
                    local=True,
                    max_datasets=MAX_DATASETS,
                    since_serial=self.options.serial)
                loader = Uploader(Job(JOB_NAME, dbh), self.logger)
                print('NEW LOAD STATE', loader.job.readable_interval)
                for row in dbh.cursor().execute(
                        "SELECT * FROM odfi_etl_status"):
                    print(row)
                for dataset in loader.job.feed.get_datasets_since_serial(self.options.serial, max_datasets=MAX_DATASETS):
                    print("\n", dataset.serial, loader.is_republish(dataset), "==", True, loader.job.readable_interval, dataset.readable_interval)
                    #self.assertTrue(loader.is_republish(dataset))


if __name__ == '__main__':
    cleanup()
    unittest.main()
