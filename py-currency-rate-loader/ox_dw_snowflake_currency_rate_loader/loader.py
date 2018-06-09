"""
Main class for running the loader.
"""
import os
from datetime import datetime, timedelta
import pid
from retrying import retry
from unicodecsv import writer
from ox_dw_db import OXDB
from ox_dw_load_state import LoadState
from ox_dw_logger import get_etl_logger
from .client import Client
from .exceptions import UserException
from .settings import (ENV, LOCK_ROOT, BASE_CURRENCY, LOAD_STATE_NAME,
                       MAX_ATTEMPTS, APP_NAME, OUTPUT_DIR, STATEMENTS,
                       WAIT_BETWEEN_ATTEMPTS)


class Loader(object):
    """
    Run this to download from currency service and load into the DW.
    """
    _currencies = {}

    def __init__(self, start_date=None, end_date=None):
        self._start_date = start_date
        self._end_date = end_date
        self.lock = pid.PidFile(
            pidname="%s.LOCK" % APP_NAME,
            piddir=LOCK_ROOT,
            enforce_dotpid_postfix=False)
        self.logger = get_etl_logger(APP_NAME)

    @property
    def dates(self):
        """
        Yield dates from start_date through end_date.
        """
        for days in range((self.end_date - self.start_date).days + 1):
            yield self.start_date + timedelta(days=days)

    @property
    def end_date(self):
        """
        Last date in range to load.
        """
        if self._end_date is None:
            self._end_date = \
                datetime.now().replace(
                    hour=0, minute=0, second=0, microsecond=0)

        return self._end_date

    @property
    def start_date(self):
        """
        If the is none then we will look to the DB to get the last date
        loaded + one day.
        """
        if self._start_date is None:
            with OXDB('SNOWFLAKE') as oxdb:
                self._start_date, = \
                    oxdb.get_executed_cursor("""
                        SELECT DATEADD(day, 1, MAX(date))
                          FROM currency_exchange_daily_fact""").fetchone()
                if self._start_date is None:
                    self._start_date = datetime.now().replace(
                        hour=0, minute=0, second=0, microsecond=0)

        return datetime(self._start_date.year, self._start_date.month,
                        self._start_date.day)

    @property
    def temp_file(self):
        """
        The file that we will write out the file to be loaded.
        """
        return os.path.join(OUTPUT_DIR, '.'.join([APP_NAME, 'csv']))

    def get_currencies(self, date):
        """
        Let's grab and cache the currency list for the default BASE_CURRENCY.
        """
        if date not in self._currencies:
            self._currencies[date] = [
                row[0]
                for row in Client(BASE_CURRENCY, date, fields=['currency'])
            ]

        return self._currencies[date]

    def load_tmp_file(self):
        """
        Will run the SQL statements to merge any new data into the DW.
        """
        with OXDB('SNOWFLAKE') as oxdb:
            for stmt in STATEMENTS:
                stmt = stmt.format(self.temp_file)
                self.logger.info("Executing:%s;", stmt)
                self.logger.info("Results:%s;", oxdb.execute(stmt))
            self.logger.info("Setting load state '%s'", LOAD_STATE_NAME)
            LoadState(oxdb.connection, variable_name=LOAD_STATE_NAME).upsert(commit=True)

    @retry(
        stop_max_attempt_number=MAX_ATTEMPTS, wait_fixed=WAIT_BETWEEN_ATTEMPTS)
    def run(self):
        """
        Performs the temporary csv file creation and SQL to load and merge.
        """
        try:
            if self.start_date > self.end_date:
                self.logger.warning(
                    "Start date(%s) is later than End date(%s)! "
                    "Nothing to do.",
                    self.start_date.strftime('%Y-%m-%d'),
                    self.end_date.strftime('%Y-%m-%d'))
                return
            with open(self.temp_file, 'wb') as temp_file:
                self.logger.info("Writing to %s", temp_file.name)
                csv_file = \
                    writer(temp_file, lineterminator='\n', encoding='utf-8')
                for date in self.dates:
                    self.logger.info(
                        "Pulling date '%s' of range('%s', '%s')...",
                        date.strftime('%Y-%m-%d'),
                        self.start_date.strftime('%Y-%m-%d'),
                        self.end_date.strftime('%Y-%m-%d'))
                    for base_currency in self.get_currencies(date):
                        for row in Client(base_currency, date):
                            csv_file.writerow(row)
            self.load_tmp_file()
        except UserException as error:
            self.logger.error(error)
        except Exception as err:
            self.logger.error(err)
            raise
