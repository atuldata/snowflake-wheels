"""
Loaded data from the api extractor output files into the dest db.
"""
import csv
import os
import sys
import subprocess
import snowflake.connector
import yaml
import pid
from ox_dw_db import OXDB
from ox_dw_load_state import LoadState
from ox_dw_logger import get_etl_logger
from .settings import (
    CREATE_LOCAL_TEMP_STMT,
    CSV_KWARGS,
    LOAD_API_RAW_STMT,
    LOG_DIR,
    APP_NAME,
    CONF_ROOT,
    ENV,
    LOCK_ROOT,
    RAW_TABLE_PREFIX,
    DB_NAMESPACE,
    PUT_STATEMENT
)

csv.field_size_limit(sys.maxsize)


class TableLoader(object):
    """
    1) Loads from a single source file into a raw table onto the dest db.
    2) Runs configured sql statements for loads into table on the dest db.
    """

    def __init__(
            self,
            table_name,
            outfile,
            schema_config,
            config_file=None):

        # Required
        self.table_name = table_name
        self.outfile = outfile
        self.schema_config = schema_config

        # Optional
        self._config_file = config_file

        # Derived properties
        self._dest_db = None
        self._logger = None
        self.table_name_raw = '_'.join([RAW_TABLE_PREFIX, self.table_name])
        self.staging_location = '@' + '.%'.join([DB_NAMESPACE, self.table_name_raw])
        self.name = '-'.join([APP_NAME, 'loader', self.table_name])
        self.config = yaml.load(open(self.config_file, 'r'))

    @property
    def config_file(self):
        """
        This is the config for a specific table.
        """
        if self._config_file is None:
            self._config_file = \
                os.path.join(
                    CONF_ROOT, APP_NAME, '.'.join([self.table_name, 'yaml']))
            if not os.path.exists(self.config_file):
                raise \
                    OSError(
                        "Can't find table config file %s!" % self.config_file)

        return self._config_file

    @property
    def dest_db(self):
        """
        Our database connection to the destination db.
        """
        if self._dest_db is None:
            self._dest_db = OXDB('SNOWFLAKE')

        return self._dest_db

    @property
    def has_dupes(self):
        """
        For the configured keys yield any rows that have dupes.
        """
        query = """
            SELECT %s, count(*) AS dupe_count
              FROM %s
          GROUP BY %s
            HAVING count(*) > 1""" % (
                ', '.join(self.config.get('KEYS')),
                self.table_name,
                ', '.join(self.config.get('KEYS')))
        for _ in self.dest_db.get_executed_cursor(query):
            self.logger.error(
                "We have dupes detected in %s. Identified by this query:%s;",
                self.table_name, query)
            return True
        return False

    @property
    def logger(self):
        """
        Where to output our logging. Defaults to the app name and table name.
        """
        if self._logger is None:
            self._logger = get_etl_logger(self.table_name, LOG_DIR)

        return self._logger

    def archive_outfile(self):
        """
        Need to move the outfile out of the way in order not to releod it.
        For now we will just compress the file.
        Previously the files were moved to a backups dir.
        """
        self.logger.info("Archiving %s", self.outfile)
        subprocess.check_call(['gzip', self.outfile])

    def upload_raw_files(self):
        """
        Stages the output file into the snowflake temp table's internal
        stage environment
        """
        statement = PUT_STATEMENT % {
            'output_file': 'file://' + self.outfile,
            'stage_location': self.staging_location
        }
        self.logger.info("Uploading file %s to table stage area", self.outfile)
        self.logger.debug(statement)
        self.dest_db.execute(statement)

    def create_raw_table(self):
        """
        Creates a temp table on the dest db to accept the new data.
        """
        columns = []
        for col_name, col_def in \
                self.schema_config.get_columns(self.table_name):
            columns.append("%s %s" % (col_name.replace('.', '_'), col_def))
        statement = \
            CREATE_LOCAL_TEMP_STMT % (
                self.table_name_raw,
                ',\n    '.join(columns))
        self.logger.info("Creating table %s" % self.table_name_raw)
        self.logger.debug(statement)
        self.dest_db.execute(statement)

    def load_raw(self):
        """
        Loads the raw data from the output file into the dest db.
        """
        columns = []
        for col_name in \
                self.schema_config.get_column_names(self.table_name):
            columns.append(col_name.replace('.', '_'))
        statement = \
            LOAD_API_RAW_STMT % {
                'table_name': self.table_name_raw,
                'columns': ',\n    '.join(columns),
                'stage_location': self.staging_location}
        self.logger.info("Bulk loading %s" % self.table_name_raw)
        self.logger.debug(statement)
        self.dest_db.execute(statement)

    def main(self):
        """
        This executes the typical workflow for loading and cleanup.
        """
        try:
            with pid.PidFile(pidname="%s.LOCK" % self.table_name, piddir=LOCK_ROOT,
                             enforce_dotpid_postfix=False) as pid_lock:
                self.logger.info("Running loader for %s table with pid:%d",
                                 self.table_name, pid_lock.pid)
                self.create_raw_table()
                self.upload_raw_files()
                self.load_raw()
                self.validate_raw_count()
                self.sync_table()
                self.archive_outfile()
        except (pid.PidFileAlreadyLockedError, pid.PidFileAlreadyRunningError):
            self.logger.warning("Unable to acquire lock for table %s. Exiting...", self.table_name)
            raise pid.PidFileAlreadyRunningError
        except Exception as error:
            self.logger.error('Exception:%s occurred during loading %s table', error, self.table_name)
            self.dest_db.rollback()
            raise Exception(error)
        finally:
            try:
                self.dest_db.close()
            except Exception:
                pass

    def sync_table(self):
        """
        Performs the defined SQL for the specific table(s).
        """
        for sync_mode in ['DESTALE', 'DELETE', 'INSERT']:
            statements = self.config.get(sync_mode)
            if not isinstance(statements, (list, tuple)):
                statements = [statements]
            for statement in statements:
                statement %= {'DEST_SCHEMA': DB_NAMESPACE,
                              'SRC_TABLE': self.table_name_raw}
                self.logger.debug(statement)
                self.dest_db.execute(statement)
        LoadState(
            self.dest_db.connection, variable_name='%s_updated' % self.table_name
        ).upsert(commit=True)

    def validate_raw_count(self):
        """
        Checks the rowcount in the raw table against the file row count.
        """
        file_count = 0
        try:
            with open(self.outfile, 'r') as in_file:
                for _ in csv.reader(in_file, **CSV_KWARGS):
                    file_count += 1
        except Exception as error:
            # Attempting to read the file in using csv and if bad data it will
            # throw an error. No need to raise.
            self.logger.error(error)
        query = """
            SELECT count(*)
              FROM %s""" % self.table_name_raw
        for table_count, in self.dest_db.get_executed_cursor(query):
            if table_count != file_count:
                self.logger.error(
                    "Row counts do not match for %s(%s) load into %s(%s)"
                    "Please inspect the snowflake logs and create a ticket for API"
                    "if due to bad data for:%s;",
                    self.outfile, file_count, self.table_name_raw, table_count,
                    self.table_name)
            else:
                self.logger.debug(
                    "Row counts match for %s(%s) load into %s(%s)",
                    self.outfile, file_count, self.table_name_raw, table_count)
