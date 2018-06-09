"""
Runs the load steps defined in the jobs yaml.
"""
import os
from datetime import datetime
from ox_dw_odfi_client import readable_interval_datetime
from ox_dw_load_state import LoadState
from ._loader_base import _LoaderBase
from ...common.content_topics import load_content_topics
from ...common.exceptions import BreakCheckError, JobNotFoundException
from ...common.odfi_schema import create_sql, copy_sql
from ...common.utils import stmt_with_args
from ...common.settings import UPLOAD_STATUS_TABLE_NAME, UPLOAD_SORT_KEY, get_conf,APP_CONF_ROOT
from ...common.rollup_util import run_rollups, queue_rollups
from .adjustmenter import Adjustmenter

GRID_FACT_RELOAD_VERSION = 'grid_fact_reload_version'
SEQ_GRID_FACT_RELOAD_VERSION = 'seq_grid_fact_reload_version'

GET_SERIALS = """
SELECT dataset_serial
FROM %s
WHERE job_name = ? AND
      odfi_feed_name = ? AND
      dataset_serial >= ?""" % UPLOAD_STATUS_TABLE_NAME
GET_SINCE_SERIAL = """
SELECT variable_value::int
FROM load_state
WHERE variable_name = ?"""


class Uploader(_LoaderBase):
    """
    For loading a single job into the DW.
    Call this class to run the main loader.
    Returns True if data was actually loaded.
    Example:
    from ox_dw_snowflake_odfi_etl import Uploader
    data_loaded = Uploader(job, logger)(max_datasets=100)
    if data_loaded:
        do other work like rollups, etc...
    """

    def __init__(self, job):
        super(Uploader, self).__init__(job)
        self.stage_table_name = '_'.join(['stage', self.job.name])
        self.load_state_serial_name = '_'.join(
            [self.job.load_state.variable_name, 'serial'])
        self.has_data = False


    def __call__(self, max_datasets=None, since_serial=None):
        """
        Iterate through the datasets and process.
        :param max_datasets: Optional. ONLY used for TESTING.
        :param since_serial: Optional. ONLY used for TESTING.
        NOTE: Above optional parameters should only be used for testing
              as they can break the upload logic leading to data issues
              downstream.
        """
        if since_serial is None:
            since_serial = self.get_since_serial()
        self.job.logger.debug("SINCE_SERIAL: %s;", since_serial)

        last_dataset = self.job.get_current_dataset()

        loaded_datasets = self.get_serials_since(since_serial)
        self.job.logger.debug("LOADED_DATASETS: %s;", loaded_datasets)

        for dataset in self.job.feed.get_datasets_since_serial(
                since_serial, max_datasets=max_datasets,
                sort_key=UPLOAD_SORT_KEY):

            if dataset.serial in loaded_datasets:
                self.job.logger.info(
                    "Dataset serial %s is already loaded, skipping.",
                    dataset.serial)
                continue

            if self.is_correct_interval(last_dataset, dataset):
                last_dataset = dataset
                start_time = datetime.utcnow()
                if self.upload(dataset):
                    self.record_state(dataset, start_time)
                    self.queue_rev_adjustment(dataset.readable_interval,self.is_republish(dataset))
                    self.queue_rollup(dataset.readable_interval)
                    continue
                else:
                    # Upload did not occur.
                    return False
            else:
                # GAP detected stop the loop
                return self.has_data

        if self.has_data:
            LoadState(self.job.dbh, self.load_state_serial_name).upsert(
                last_dataset.serial, commit=True)

        return self.has_data

    @property
    def status_table(self):
        """
        The status table to use.
        """
        return UPLOAD_STATUS_TABLE_NAME

    def queue_rev_adjustment(self,readable_interval,is_republish):
        utc_date_sid = str(readable_interval).split(" ")[0].replace("-", "")
        Adjustmenter.queue_rev_adjustment(utc_date_sid,readable_interval,is_republish)

    def queue_rollup(self, readable_interval):
        if 'ROLLUP_CONFIG' in self.job.config:
            for rollup_name in self.job.config['ROLLUP_CONFIG']:
                self.job.logger.debug("queue %s ROLLUP" % rollup_name)
                queue_rollups(self.job.dbh, self.job.config['ROLLUP_CONFIG'], rollup_name, readable_interval)

            if 'QUEUE_DEPENDENT_ROLLUP_JOB' in self.job.config['ROLLUP_CONFIG']:
                for job_name in self.job.config['ROLLUP_CONFIG']['QUEUE_DEPENDENT_ROLLUP_JOB']:
                    dependent_job_conf=get_conf(job_name)
                    for rollup_name in dependent_job_conf['ROLLUP_CONFIG']:
                        queue_rollups(self.job.dbh, self.job.config['ROLLUP_CONFIG'], rollup_name, readable_interval)

    def are_no_dim_duplicates(self):
        """
        Are there any columns in the dim_duplicates table?
        """
        for count, in self.job.dbh.cursor().execute(
                "SELECT count(*) FROM dim_duplicates"):
            if count > 0:
                self.job.logger.error(
                    "%s duplicates found in dim_duplicates!!!", count)
                return False

        self.job.logger.debug("There are no dim duplicates.")
        return True

    def create_stage_table(self, dataset):
        """
        Creates a temporary table for the purpose of loading the datasets
        into the DW.
        """
        self.drop_stage_table()
        stmt = create_sql(dataset.schema, self.stage_table_name)
        self.job.logger.debug(stmt)
        self.job.dbh.cursor().execute(stmt)
        self.job.logger.debug("%s is created.", self.stage_table_name)

    def dependency_check(self, readable_interval):
        """
        Checks for dependancies that they are completed for the same or greater
        readable_interval.
        """
        passed = True
        for dep_job in self.job.depends_on:
            if dep_job.readable_interval < readable_interval:
                self.job.logger.warning(
                    "%s is waiting on %s for readable_interval %s",
                    self.job.name, dep_job.name, readable_interval)
                passed = False

        return passed

    def drop_stage_table(self):
        """
        Drop the staging table if it exists.
        """
        stmt = "DROP TABLE IF EXISTS %s" % self.stage_table_name
        self.job.logger.debug(stmt)
        self.job.dbh.cursor().execute(stmt)
        self.job.logger.debug("%s has been dropped if it existed.",
                              self.stage_table_name)

    def execute_etl_statements(self, dataset, kwargs=None):
        """
        Executes all statements configured.
        """
        if 'ETL_STATEMENTS' not in self.job.config:
            self.job.logger.warning("No ETL_STATEMENTS found to run!")
            return

        default_kwargs = {
            'DATETIME':
            dataset.readable_interval.strftime('%Y-%m-%d %H:%M:%S'),
            'JOB_NAME': self.job.name,
            'READABLE_INTERVAL': dataset.meta['readableInterval'],
            'UTC_DATE_SID': int(dataset.readable_interval.strftime('%Y%m%d')),
            'UTC_HOUR_SID': int(dataset.readable_interval.strftime('%H'))
        }

        if kwargs is not None:
            default_kwargs.update(kwargs)

        kwargs = default_kwargs

        for index, stmt_def in enumerate(self.job.config['ETL_STATEMENTS'], 1):
            stmt = stmt_def
            execute_stmt = True
            if isinstance(stmt_def, dict):
                if 'break_check' in stmt_def:
                    execute_stmt = False
                    ind = 0
                    stmt, args = stmt_with_args(stmt_def['break_check'],
                                                kwargs)
                    self.job.logger.debug('BREAK_CHECK: %s: %s;', stmt, args)
                    cursor = self.job.dbh.cursor()
                    cursor.execute(
                        stmt % {'STAGING_TABLE': self.stage_table_name}, args)
                    fields = [field[0].lower() for field in cursor.description]
                    for ind, row in enumerate(cursor, 1):
                        self.job.logger.error(
                            "%s; %s;", stmt_def.get('label', 'Results'),
                            str(dict(list(zip(fields, row)))))
                    if bool(ind):
                        raise BreakCheckError(
                            "Results found in break_check: %s;" %
                            stmt_def['break_check'])
                if 'continue_check' in stmt_def:
                    stmt, args = stmt_with_args(stmt_def['continue_check'],
                                                kwargs)
                    self.job.logger.debug('CONTINUE_CHECK: %s: %s', stmt, args)
                    for passed, in self.job.dbh.cursor().execute(
                            stmt % {'STAGING_TABLE': self.stage_table_name},
                            args):
                        if not passed:
                            self.job.logger.debug("CONTINUE_CHECK failed!!!")
                            execute_stmt = False
            if 'stmt' in stmt_def:
                stmt = stmt_def['stmt']
            if execute_stmt:
                stmt, args = stmt_with_args(stmt, kwargs)
                self.job.logger.debug(
                    'ETL_STATEMENT[%s] %s, %s', index,
                    stmt % {'STAGING_TABLE': self.stage_table_name}, args)
                self.job.dbh.cursor().execute(
                    stmt % {'STAGING_TABLE': self.stage_table_name}, args)

    def get_serials_since(self, since_serial):
        """
        Pull previously recorded serials.
        """
        return set(dataset_serial
                   for dataset_serial, in self.job.dbh.cursor().execute(
                       GET_SERIALS, (
                           self.job.name,
                           self.job.feed.name,
                           since_serial,
                       )))

    def get_since_serial(self):
        """
        Returns the last loaded serial.
        """
        self.job.logger.debug("get_serial(): %s; %s", GET_SINCE_SERIAL,
                              (self.load_state_serial_name, ))
        for dataset_serial, in self.job.dbh.cursor().execute(
                GET_SINCE_SERIAL, (self.load_state_serial_name, )):
            return dataset_serial

        raise JobNotFoundException(self.job.name)

    def is_correct_interval(self, last_dataset, dataset):
        """
        The next dataset interval needs to be delta one unit from the previous.
        """
        if last_dataset.next_readable_interval == dataset.readable_interval:
            self.job.logger.debug(
                "Readable interval %s is the correct one to load.",
                dataset.readable_interval)
            return True
        if self.is_republish(dataset):
            self.job.logger.debug(
                "Readable interval %s is a republish so this is fine.",
                dataset.readable_interval)
            return True

        self.job.logger.warning(
            "JOB_NAME:%s; FEED_NAME:%s; SERIAL:%s; "
            "%s != %s; Missing interval!", self.job.name, self.job.feed_name,
            dataset.serial, dataset.readable_interval,
            last_dataset.next_readable_interval)
        return False

    def is_republish(self, dataset):
        """
        If the dataset readable_interval is older than of the same as the
        recorded load_state for this job then we are republishing.
        """
        self.job.logger.debug("IS_REPUBLISH '%s' >= '%s'",
                              self.job.readable_interval,
                              dataset.readable_interval)
        return self.job.readable_interval >= dataset.readable_interval

    def load_content_topics(self):
        """
        If a dependancy the load content topics.
        """
        if self.job.config.get('NEEDS_CONTENT_TOPICS', False):
            self.job.logger.debug("Loading content_topics...")
            load_content_topics(self.stage_table_name, self.job.dbh,
                                self.job.logger)
            self.job.logger.debug("Finished loading content_topics...")

    def load_stage_table(self, dataset):
        """
        Loads the dataset files into the staging table.
        """
        stmt = copy_sql(dataset, self.stage_table_name)
        self.job.logger.debug(stmt)
        self.job.dbh.cursor().execute(stmt)
        self.job.dbh.commit()
        self.job.logger.debug("%s is loaded", self.stage_table_name)

    def record_state(self, dataset, start_time):
        """
        Save checkpoints to the DB.
        """
        self.add_status(dataset, start_time)
        self.update_load_state(dataset)
        self.job.dbh.commit()

    def upload(self, dataset):
        """
        Here we are doing the ELT processing.
        """
        if dataset.has_part_files:
            if self.upload_checks_pass(dataset):
                self.create_stage_table(dataset)
                self.load_stage_table(dataset)
                self.load_content_topics()
                self.execute_etl_statements(dataset)
                self.has_data = True
            else:
                return False
        else:
            self.add_empty_dataset(dataset)

        return True

    def upload_checks_pass(self, dataset):
        """
        A few final checks before uploading.
        """
        if not self.is_staged(dataset):
            return False
        if not self.dependency_check(dataset.readable_interval):
            return False
        if not self.are_no_dim_duplicates():
            return False

        return True

    def update_load_state(self, dataset):
        """
        Update loadstate(s) if needed.
        """
        # If we are not a republish the update the load_state
        if not self.is_republish(dataset):
            variable_value = readable_interval_datetime(
                dataset.meta['readableInterval'])
            self.job.logger.info("DATASET %s(%s) is loaded", variable_value,
                                 dataset.serial)
            self.job.load_state.upsert(variable_value)
            # If any, then set the AUXILIARY_LOAD_STATE_VARS
            for variable_name in self.job.config.get(
                    'AUXILIARY_LOAD_STATE_VARS', []):
                LoadState(self.job.dbh, variable_name).upsert(variable_value)
        else:
            # Increment grid_fact_reload_version load state variable
            LoadState(self.job.dbh, GRID_FACT_RELOAD_VERSION).increment_by_seq(
                SEQ_GRID_FACT_RELOAD_VERSION)
