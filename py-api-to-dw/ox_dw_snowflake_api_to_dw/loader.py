"""
Loaded data from the api extractor output files into the dest db.
"""
import fnmatch
from multiprocessing import Pool
import os
import time
import pid
import traceback
from ox_dw_logger import get_etl_logger
from .settings import (
    FILE_FORMAT,
    HARMONIZER_NAME,
    MAX_LOADERS,
    APP_NAME,
    OUTPUT_DIR,
    LOCK_ROOT,
    LOADER_APP_NAME,
    LOG_DIR
)
from .schema_config import SchemaConfig
from .table_loader import TableLoader


def get_output_files(table_name):
    """
    Yields back output files for a given table_name ordered by date.
    """
    for root, _, files in os.walk(OUTPUT_DIR):
        if any(path_ in root for path_ in [APP_NAME, HARMONIZER_NAME]) and \
                table_name in root:
            for output_file in fnmatch.filter(files, '*.%s' % FILE_FORMAT):
                yield os.path.join(root, output_file)


def load_table(table_name, schema_config):
    """
    Loads a table.
    """
    for output_file in get_output_files(table_name):
        try:
            table_loader = TableLoader(table_name, output_file, schema_config)
            table_loader.main()
        except (pid.PidFileAlreadyLockedError, pid.PidFileAlreadyRunningError):
            return False, "Unable to acquire lock for table %s. Exiting..." % table_name
        except (KeyboardInterrupt, SystemExit) as exc:
            return False, 'Shutting down via %s...' % str(exc)
        except Exception:
            # We don't want to raise since this need to return to the callback
            return False, traceback.format_exc()
    return True, None


class APILoader(object):
    """
    This is a wrapper around the multiprocessing of the table loads.
    This will run forever until killed.
    """

    def __init__(self, schema_file, tables=None):
        # Required
        self.schema_file = schema_file

        # Constructor setup
        self.lock = pid.PidFile(pidname="%s.LOCK" % LOADER_APP_NAME, piddir=LOCK_ROOT,
                                enforce_dotpid_postfix=False)
        self.logger = get_etl_logger(LOADER_APP_NAME, LOG_DIR)
        self.schema_config = SchemaConfig(self.schema_file)

        # Optional
        self.tables = tables
        if not bool(self.tables):
            self.tables = self.schema_config.configured_tables

    def get_tables(self):
        """
        Endless generator of the table_names.
        """
        while True:
            for table_name in sorted(self.tables):
                yield table_name
            # Throttle the restart of the loop
            time.sleep(10)

    def start_loader(self):
        """
        Will load multiple tables at a time in a continuous loop.
        """
        try:
            self.logger.info("Starting the loader application.")
            pool = Pool(processes=MAX_LOADERS)
            active = {}
            for table_name in self.get_tables():
                self.logger.info("Executing loader for table %s", table_name)
                if not active.get(table_name, False):
                    active[table_name] = True

                    def callback(ret_tup, table_name=table_name):
                        """
                        This is used for keeping track of which tables are
                        currently running.
                        """
                        (successful, msg) = ret_tup
                        if not successful:
                            self.logger.error(
                                "%s error for %s", table_name, msg)
                        active[table_name] = False

                    pool.apply_async(
                        load_table, args=[table_name, self.schema_config],
                        callback=callback)
            self.logger.info("Closing the process pool")
            pool.close()
        except (KeyboardInterrupt, SystemExit, Exception) as error:
            self.logger.info("Terminating the process pool")
            pool.terminate()
            self.logger.error("Exception error:%s;", error)
            raise Exception(error)
        finally:
            self.logger.info("Joining the process pool")
            pool.join()
            self.logger.info("Closing the loader application.")
