"""
For pulling options json from rest api
"""
from itertools import permutations
import os
import requests
import pid
import unicodecsv as csv
from ox_dw_load_state import LoadState
from ox_dw_logger import get_etl_logger
from ox_dw_db import OXDB
from .settings import (
    ENV,
    LOCK_ROOT,
    DELIMITER,
    ENCLOSED,
    ESCAPE,
    LOAD_STATE_TEMPLATE,
    APP_NAME,
    NULL,
    OUTPUT_DIR,
    COPY_STMT,
    CREATE_STMT,
    INSERT_STMT,
    MERGE_STMT,
    QUERY_STMT,
    PUT_STMT,
    DB_NAMESPACE,
    DEFAULT_PATH
)


class TableLoader(object):
    """
    Requires a table_name and it's config and will write out the file to bulk
    load into the DW, load it, run any transformations configured and finally
    merge into the destination table.
    """
    def __init__(self, table_name, config):
        self.table_name = table_name
        self.config = config

        self.tmp_table_name = '_'.join(['tmp', self.table_name])
        self.name = '-'.join([APP_NAME, self.table_name])

        self.lock = pid.PidFile(pidname="%s.LOCK" % self.name,
                                piddir=LOCK_ROOT, enforce_dotpid_postfix=False)
        self.logger = get_etl_logger(self.name)
        self.tmp_file = \
            os.path.join(OUTPUT_DIR, '.'.join([self.table_name, 'csv']))

        # These values are pivoted from the config and used for kwargs.
        self.keys_keys = \
            [list(item.keys())[0] for item in self.config.get('keys', [])]
        self.keys_values = \
            [list(item.values())[0] for item in self.config.get('keys', [])]
        self.values_keys = \
            [list(item.keys())[0] for item in self.config.get('values', [])]
        self.values_values = \
            [list(item.values())[0] for item in self.config.get('values', [])]

        # These items can be overridden in the config if needed but defaults
        # usually work fine.
        self.join_keys = self.config.get('join_keys', self.keys_values)
        self.join_values = self.config.get('join_values', self.values_values)
        self.key_columns = self.config.get('key_columns', self.join_keys)
        self.set_columns = self.config.get('set_columns', self.values_values)

        # These are used for kwargs.
        self.destination_fields = self.keys_values + self.values_values
        self.source_fields = self.keys_keys + self.values_keys
        self.key_sep = self.config.get('key_sep')

        self._kwargs = None
        self._oxdb = None

    @property
    def kwargs(self):
        """
        These are any and all of the kwargs in the SQL's
        """
        if self._kwargs is None:
            self._kwargs = {
                'delimiter': DELIMITER,
                'enclosed': ENCLOSED,
                'escape': ESCAPE,
                'dest_columns': ', '.join(self.destination_fields),
                'dest_keys': ' AND '.join(
                    ["dest.%s IS NULL" % val for val in self.join_keys]),
                'dest_table': self.table_name,
                'join_keys': ' AND\n       '.join(
                    ["temp.%s = dest.%s" % (val, val)
                     for val in self.join_keys]),
                'join_values': ' AND\n            '.join(
                    ["EQUAL_NULL(temp.%s, dest.%s)" % (val, val)
                     for val in self.join_values]),
                'key_column': self.keys_values[0],
                'key_columns': ', '.join(self.key_columns),
                'null': NULL,
                'set_columns': ',\n        '.join(
                    ["%s = temp.%s" % (val, val)
                     for val in self.set_columns]),
                'source_columns': ', '.join(self.source_fields),
                'source_table': self.config.get('sources')[0],
                'temp_columns': ', '.join(
                    ["temp.%s" % _
                     for _ in self.destination_fields]),
                'temp_file': 'file://' + self.tmp_file,
                'temp_table': self.tmp_table_name,
                'text_column': self.values_values[0],
                'stage_location': '@' + '.%'.join([DB_NAMESPACE,
                                                   self.tmp_table_name])
            }

        return self._kwargs

    @property
    def oxdb(self):
        """
        Our DB Util.
        """
        if self._oxdb is None:
            self._oxdb = OXDB('SNOWFLAKE')

        return self._oxdb

    def main(self):
        """
        Run for the configured table to load.
        """
        try:
            self.logger.info("START Create: %s;", self.tmp_file)
            if self.config.get('permutations'):
                self._write_permutations()
            else:
                self._write_options()
            self.logger.info("FINISH Create: %s", self.tmp_file)

            self._load_options()

            LoadState(
                self.oxdb.connection,
                variable_name=LOAD_STATE_TEMPLATE % self.table_name.upper()
                ).upsert(commit=True)
        except Exception as error:
            self.logger.error("Options Loader failed for %s. Error %s",
                              self.table_name.upper(), error)
            self.oxdb.rollback()
            raise Exception("%s options loader failed. Error %s"
                            % (self.table_name.upper(), error))
        finally:
            try:
                self.oxdb.close()
            except Exception:
                pass

    def _get_keys(self, key_options):
        """
        Will yield all the key sets from the options.
        Recurses on split keys.
        """
        do_yield = True
        keys = []
        for index, key_option in enumerate(key_options):
            if self.key_sep is not None and self.key_sep in key_option:
                for option in key_option.split(self.key_sep):
                    if option != '':
                        key_options_copy = list(key_options)
                        key_options_copy[index] = option
                        for keys in self._get_keys(key_options_copy):
                            yield keys
                do_yield = False
            else:
                keys.append(key_option)
        if do_yield:
            yield keys

    def _load_options(self):
        """
        Will process a single table load.
        """
        self.logger.info("START Load: %s;", self.table_name)
        for stmt in (
                [CREATE_STMT, PUT_STMT, COPY_STMT] +
                [_ for _ in self.config.get('transformations', [])] +
                [INSERT_STMT, MERGE_STMT]):
            stmt %= self.kwargs
            self.logger.debug(stmt)
            self.oxdb.execute(stmt)
        self.logger.info("FINISH Load: %s", self.table_name)

    def _write_options(self):
        """
        Grabs the options data from the rest api and writes to the tmp_file.
        """
        with open(self.tmp_file, 'wb') as tmp_file:
            writer = \
                csv.writer(
                    tmp_file, delimiter=DELIMITER, quoting=csv.QUOTE_NONE,
                    quotechar=ENCLOSED, escapechar=ESCAPE, encoding='utf-8')
            for source in self.config.get('sources'):
                url = os.path.join(ENV['API_RIAK_HOST'],
                                   self.config.get('source_path',
                                                   DEFAULT_PATH), source)
                self.logger.info("Fetching from %s", url)
                response = requests.get(url)
                self.logger.debug("Request response: %d", response.status_code)
                seen_keys = set()
                for option in response.json():
                    key_options = []
                    for key in self.keys_keys:
                        key_option = ''
                        if key in option:
                            key_option = option.get(key)
                            if not isinstance(key_option, int) and \
                                    (key_option is None or
                                     len(key_option) < 1):
                                key_option = 'null'
                        key_options.append(str(key_option))
                    for keys in self._get_keys(key_options):
                        key = '.'.join(keys)
                        if key in seen_keys:
                            continue
                        seen_keys.add(key)
                        row = keys
                        for value in self.values_keys:
                            value_option = ''
                            if value in option:
                                value_option = option.get(value)
                                if not isinstance(value_option, int) and \
                                        (value_option is None or
                                         len(value_option) < 1):
                                    value_option = NULL
                            row.append(value_option)
                        writer.writerow(row)

    def _write_permutations(self):
        """
        Selects data from the DW source, then runs the permutations.
        """
        query = QUERY_STMT % self.kwargs
        self.logger.debug("%s:%s;", self.tmp_table_name, query)
        rows = self.oxdb.get_executed_cursor(query).fetchall()

        with open(self.tmp_file, 'wb') as tmp_file:
            writer = \
                csv.writer(
                    tmp_file, delimiter=DELIMITER, quoting=csv.QUOTE_NONE,
                    quotechar=ENCLOSED, escapechar=ESCAPE, encoding='utf-8')
            lookup = dict(rows)
            for index in range(1, len(rows) + 1):
                for row in permutations(lookup.keys(), index):
                    writer.writerow([
                        ','.join([str(_) for _ in row]),
                        ','.join([lookup.get(_) for _ in row])])
