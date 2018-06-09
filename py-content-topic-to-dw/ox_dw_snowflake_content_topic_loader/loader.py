"""
Main class for running the loader.
"""
import csv
import os
import traceback
import pid
import ujson as json
from ox_dw_db import OXDB
from ox_dw_load_state import LoadState
from ox_dw_logger import get_etl_logger
from .settings import (
    APP_NAME, OUTPUT_DIR, LOAD_STATE_NAME, CONTENT_TOPICS_JSON,
    CUSTOMERS_DIR, STAGING_TABLE_NAME, STATEMENTS, DELIMITER, ENV,
    LOCK_ROOT, STAGING_LOCATION, DEST_TABLE_NAME
)


class Loader(object):
    """
    Run this to load content topics into the DW.
    """

    def __init__(self):
        self.lock = pid.PidFile(pidname="%s.LOCK" % APP_NAME,
                                piddir=LOCK_ROOT, enforce_dotpid_postfix=False)
        self.logger = get_etl_logger(APP_NAME)

    @property
    def load_file(self):
        """
        The file that we will write out the file to be loaded.
        """
        return os.path.join(OUTPUT_DIR, '.'.join([APP_NAME, 'csv']))

    @property
    def sql_args(self):
        """
        Some dynamically placed variables into the SQL statements.
        """
        return {
            'load_file': 'file://' + self.load_file,
            'stage_table_name': STAGING_TABLE_NAME,
            'delimiter': DELIMITER,
            'stage_location': STAGING_LOCATION,
            'dest_name': DEST_TABLE_NAME
        }

    def create_load_file(self):
        """
        Will parse each of the content_topics.json files and create the
        load_file.
        """
        self.logger.info("Creating %s", self.load_file)
        with open(self.load_file, 'w') as out_file:
            csv_file = \
                csv.writer(
                    out_file, delimiter=DELIMITER,
                    quoting=csv.QUOTE_NONE, escapechar='', quotechar='')
            for root, _, files in os.walk(CUSTOMERS_DIR):
                for file_name in files:
                    json_file = os.path.join(root, file_name)
                    if self.is_valid_file(json_file):
                        platform_id = os.path.basename(root)
                        with open(os.path.join(root, json_file),
                                  'r') as json_in:
                            seen_ids = {}
                            for doc in json.load(json_in):
                                if doc.get('id') in seen_ids:
                                    continue
                                seen_ids[doc.get('id')] = True
                                csv_file.writerow([
                                    platform_id,
                                    doc.get('id'),
                                    doc.get('name')
                                ])

    def is_valid_file(self, json_file):
        """
        Checks the file to see if it appropriate for our use.
        Warns if not.
        :param str: json file
        :returns boolean:
        """
        if not json_file.endswith(CONTENT_TOPICS_JSON):
            self.logger.warning("%s is not %s!", json_file,
                                CONTENT_TOPICS_JSON)
            return False
        if os.stat(json_file).st_size == 0:
            self.logger.warning("%s is empty!", json_file)
            return False
        return True

    def run(self):
        """
        Performs the temporary csv file creation and SQL to load and merge.
        """
        try:
            self.lock.create()
            self.create_load_file()
            self.run_sql()
        except (pid.PidFileAlreadyLockedError, pid.PidFileAlreadyRunningError):
            self.logger.warning("Unable to get lock.Exiting...")
        except Exception:
            # Log the exception then raise it.
            self.logger.error(traceback.format_exc())
            raise

    def run_sql(self):
        """
        Will run the SQL statements to merge any new data into the DW.
        """
        with OXDB('SNOWFLAKE') as oxdb:
            for stmt in STATEMENTS:
                stmt %= self.sql_args
                self.logger.info("Executing:%s;", stmt)
                self.logger.info("Results:%s;", oxdb.execute(stmt))
            self.logger.info("Setting load state '%s'", LOAD_STATE_NAME)
            LoadState(oxdb.connection, variable_name=LOAD_STATE_NAME).upsert(commit=True)
