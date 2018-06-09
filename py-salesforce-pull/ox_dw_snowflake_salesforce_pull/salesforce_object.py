"""
Loads SalesForce object by name
"""
import os
import re
import time
from requests.exceptions import SSLError
from retrying import retry
from .settings import (CONFIG, DELIMITER, GET_COLUMNS, INSERT_MISSING_COLUMNS,
                       LOGGER, MAX_ATTEMPTS, TEMP_FILE_DIR,
                       WAIT_BETWEEN_ATTEMPTS, DB_NAME, DB_SCHEMA)
from .util import check_val, salesforce_type_to_vsql_type


class SalesForceObject(object):
    """
    Runs for a single object.
    Since we are not doing multiprocessing at this time we have the salesforce
    client, oxdb and logger objects be from a shared source.
    """
    _field_dict = None
    _tmp_csv_file = None

    def __init__(self, name, client, oxdb):
        self.name = name
        self.client = client
        self.oxdb = oxdb

        self._compare_cols()

    @property
    def col_list(self):
        """
        TODO
        """
        return list(self.field_dict)

    @property
    def field_dict(self):
        """
        TODO
        """
        if self._field_dict is None:
            self._field_dict = {
                field['name']: [
                    field[key] for key in ('name', 'type', 'length',
                                           'byteLength', 'precision', 'scale')
                ]
                for field in self.fields
            }
        return self._field_dict

    @property
    def fields(self):
        """
        TODO
        """
        return getattr(self.client, self.name).describe()['fields']

    @property
    def table_name(self):
        """
        Name of the table that the object data is going to be stored into.
        """
        return "_".join(["SF", self.name])

    @property
    def tmp_csv_file(self):
        """
        File used to copy the data into the data warehouse.
        """
        if self._tmp_csv_file is None:
            self._tmp_csv_file = \
                '_'.join([TEMP_FILE_DIR, self.name, str(os.getpid())]) \
                + ".csv"
        return self._tmp_csv_file

    def cleanup(self):
        """
        Close any open objects or leftovers such as tmp files.
        """
        if os.path.exists(self.tmp_csv_file):
            LOGGER.debug("Removing tmp_csv_file:%s;", self.tmp_csv_file)
            os.remove(self.tmp_csv_file)

    def copy_salesforce_table(self):
        """
        Loads from the temporary file we output from the salesforce data
        fetch.
        """
        stage_dir = 'file://' + self.tmp_csv_file
        stage_stmt = """PUT %s @%%%s""" % (stage_dir, self.table_name)
        LOGGER.info("Stage upload statement: %s", stage_stmt)
        self.oxdb.execute(stage_stmt)
        LOGGER.info("Uploaded file_content %s to stage %s location.",
                    stage_dir, self.table_name)

        copy_stmt = """
         COPY INTO %s
             FILE_FORMAT = (
                 TYPE = CSV
                 FIELD_DELIMITER = '%s')
             ON_ERROR = ABORT_STATEMENT
             PURGE = TRUE""" % (self.table_name, DELIMITER)
        LOGGER.debug(copy_stmt)
        self.oxdb.execute(copy_stmt)

    def create_salesforce_table(self):
        """
        Creates the temporary storage table from the csv import.
        """
        pretty_str = "\n"
        position_index = 0
        create_stmt = """
            CREATE TABLE IF NOT EXISTS %s (%s""" % (self.table_name,
                                                    pretty_str)

        for col in self.col_list:
            field_name = self.field_dict[col][0]
            field_type = \
                salesforce_type_to_vsql_type(
                    self.field_dict[col][1],
                    self.field_dict[col][3],
                    self.field_dict[col][4],
                    self.field_dict[col][5])
            if position_index > 0:
                create_stmt += ",%s" % pretty_str
            create_stmt += "%s %s" % (field_name, field_type)
            position_index += 1

        create_stmt += ")%s" % pretty_str
        LOGGER.debug(create_stmt)
        self.oxdb.execute(create_stmt)

        grant_stmt = "GRANT select ON %s TO ROLE public" % self.table_name
        LOGGER.debug(grant_stmt)
        self.oxdb.execute(grant_stmt)

    def drop_existing_table(self):
        """
        Drops the temp table used to store the csv data pulled from SalesForce.
        """
        drop_stmt = "DROP TABLE IF EXISTS %s CASCADE" % self.table_name
        LOGGER.debug(drop_stmt)
        self.oxdb.execute(drop_stmt)

    def main(self):
        """
        This will call in order the steps needed to fetch, load and
        ultimately store in the DB the SalesForce data.
        """
        for step in ('query_to_file', 'drop_existing_table',
                     'create_salesforce_table', 'copy_salesforce_table'):
            start_time = time.time()
            LOGGER.info("Starting %s[%s]...", step, self.name)
            getattr(self, step)()
            LOGGER.info("Finished %s[%s] in %s seconds", step, self.name,
                        (time.time() - start_time))

    @retry(
        stop_max_attempt_number=MAX_ATTEMPTS, wait_fixed=WAIT_BETWEEN_ATTEMPTS)
    def query_to_file(self):
        """
        Fetches the SalesForce data using the client and stored to a csv file.
        output temp csv file format would be: "col1","col2",...
        """
        query = """
            SELECT %s
              FROM %s""" % (','.join(self.col_list), self.name)
        LOGGER.debug("Running SQL query %s", query)
        LOGGER.debug("TMP file name %s", self.tmp_csv_file)

        row_cnt = 0
        attempt = 1
        query_result = None
        while attempt < CONFIG['MAX_ATTEMPTS']:
            try:
                query_result = self.client.query(query)
                break
            except (ConnectionError, SSLError) as error:
                LOGGER.error("Attempt #%s: %s", attempt, error)
                attempt += 1
                LOGGER.warning("Sleeping for %s seconds before re-attempting.",
                               CONFIG['WAIT_BETWEEN_ATTEMPTS'])
                time.sleep(CONFIG['WAIT_BETWEEN_ATTEMPTS'])
        if not query_result:
            LOGGER.error("No query_results!!!")
            raise ValueError

        tmp_file = open(self.tmp_csv_file, "w")
        while True:
            # write records into tmp file
            for record in query_result['records']:
                row_cnt += 1
                # Replace tabs, returns, MS carriage returns(\r\n) with space
                csv_rec = \
                    DELIMITER.join(
                        [re.sub('\r\n|\n|\t|\r', ' ',
                                check_val(record[c],
                                          self.field_dict[c][1])).replace(
                                              '\0', '')
                         for c in self.col_list])
                tmp_file.write(csv_rec + "\n")
                # get next batch of records if needed
            if query_result['done'] is False:
                next_records_url = query_result['nextRecordsUrl']
                query_result = self.client.query_more(next_records_url, True)
            else:
                break

        tmp_file.close()

    def _compare_cols(self):
        """
        TODO: Does some validation of the SalesForce data?
        """
        f_names = {}
        for field in self.fields:
            f_names.setdefault(field['name'], field['name'])

        LOGGER.debug("%s; (%s, %s)", GET_COLUMNS.format(DB_NAME.upper()),
                     DB_SCHEMA.upper(), self.table_name.upper())
        cur_db_cols = {}
        for row in self.oxdb.get_executed_cursor(
                GET_COLUMNS.format(DB_NAME.upper()),
                (DB_SCHEMA.upper(), self.table_name.upper())):
            cur_db_cols.setdefault(row[0], row[1])
        for col, pos in cur_db_cols.items():
            if col not in f_names:
                LOGGER.debug("Missing column %s.%s at pos: %s",
                             self.table_name, col, str(pos))
                self._update_monitor(col)

    def _update_monitor(self, col):
        """
        TODO: Again, not sure what the monitor table is used for and
        why we are updating it.
        """
        LOGGER.debug("%s; (%s, %s)", INSERT_MISSING_COLUMNS, self.table_name,
                     col)
        self.oxdb.execute(
            INSERT_MISSING_COLUMNS, (self.table_name, col), commit=True)

    __del__ = cleanup
