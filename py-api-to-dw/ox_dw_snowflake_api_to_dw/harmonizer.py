"""
Re-exporting data from the API DB.
"""
import sys
import psycopg2
import pid
from progressbar import ProgressBar, widgets, UnknownLength
from retrying import retry, RetryError
from ox_dw_logger import get_etl_logger
from .extractor import Extractor
from .settings import DEFAULT_SCHEMA_FILE, HARMONIZER_NAME, LOCK_ROOT, ENV
from .schema_config import SchemaConfig

API_JSONDB_CONFIG = ENV.get('API_JSONDB_CONFIG')
LOGGER = get_etl_logger(HARMONIZER_NAME)
MAX_ATTEMPTS = 5
WAIT_BETWEEN_ATTEMPTS = 2000  # ms

# Object name to JSONDB table mapping.
# Most of the time the object and table name are the same.
# This is for when it doesn't
# Keep this here since it is specific to the harmonizer and may change when
# switching to postgres someday.
OBJECT_TYPE_TO_TABLE = {
    'order': 'order_',
    'user': 'user_'
}


def retry_if_db_error(exception):
    """
    Only retry on a psycopg2.Error. Else raise error immediately.
    """
    if isinstance(exception, psycopg2.Error):
        LOGGER.warning(exception)
        sys.stderr.write("\nWARNING: %s\n" % str(exception))
        return True

    return False


class Harmonizer(object):
    """
    Will pull all of the data for a given object_type and modified_date range
    for load into the DW.
    """
    _extractor = None

    def __init__(
            self, object_type, start_time, end_time,
            where_clause=None, schema_file=DEFAULT_SCHEMA_FILE):
        self.object_type = object_type
        self.schema_file = schema_file
        self.schema_config = SchemaConfig(self.schema_file)
        self.lock = pid.PidFile(pidname="%s.LOCK" % HARMONIZER_NAME,
                                piddir=LOCK_ROOT, enforce_dotpid_postfix=False)
        self.logger = LOGGER

        # Let's assemble the where clause.
        if start_time is not None:
            modified_date = \
                "cast_tz(obj->>'modified_date') BETWEEN '%s' AND '%s'" % (
                    start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    end_time.strftime("%Y-%m-%d %H:%M:%S"))
            if where_clause is not None:
                where_clause = "%s AND %s" % (where_clause, modified_date)
            else:
                where_clause = modified_date
        self.where_clause = where_clause

    @property
    def extractor(self):
        """
        This readies and writes out the converted json to a file for loading.
        """
        if self._extractor is None:
            self._extractor = \
                Extractor(HARMONIZER_NAME, self.schema_config, self.logger)

        return self._extractor

    @property
    def table_name(self):
        """
        Returns the table name including db_name.
        """
        return OBJECT_TYPE_TO_TABLE.get(self.object_type, self.object_type)

    def get_query(self):
        """
        Returns the query for a given db_name.
        """
        query = "SELECT obj FROM %s" % self.table_name
        if self.where_clause is not None:
            query += " WHERE %s" % self.where_clause

        return query

    def get_rows(self):
        """
        yield the json from the db.
        """
        conn = psycopg2.connect(**API_JSONDB_CONFIG)
        with conn.cursor() as cursor:
            cursor.execute(self.get_query())
            for obj, in cursor:
                yield {"action": "harmonize", "object": obj}

    def run(self):
        """
        This is our main method to actually does the work.
        """
        try:
            self.lock.create()
            sys.stderr.write('Using: %s;\n' % self.get_query())
            if self.object_type not in self.schema_config.configured_objects:
                sys.stderr.write(
                    "SKIPPING %s because it is not configured.\n" %
                    self.object_type)
                return
        except (pid.PidFileAlreadyLockedError, pid.PidFileAlreadyRunningError):
            return

        @retry(stop_max_attempt_number=MAX_ATTEMPTS,
               wait_fixed=WAIT_BETWEEN_ATTEMPTS,
               retry_on_exception=retry_if_db_error)
        def _run():
            """
            This will retry up to MAX_ATTEMPTS. Will only retry on a
            psycopg2.Error.
            """
            self.logger.info('STARTING %s;', self.object_type)
            progress_bar = \
                ProgressBar(
                    widgets=[
                        'Object Type: %s;' % self.object_type,
                        ' ', widgets.AnimatedMarker(),
                        ' ', widgets.Counter(),
                        ' ', widgets.Timer()],
                    max_value=UnknownLength)
            progress_bar.update(0)
            for index, row in enumerate(self.get_rows()):
                progress_bar.update(index)
                self.extractor.update_handler(row)
            self.extractor.flush_table_data()
            sys.stderr.write('\n')
            self.logger.info('FINISHED %s;', self.object_type)

        try:
            _run()
        except RetryError as error:
            self.logger.error(error)
            raise
