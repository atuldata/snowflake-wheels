"""
This is what does the actual parsing of the incoming data and outputs nice
 loader files.
"""
import csv
from datetime import datetime
import os
import sys
import sqlite3
import msgpack
from .settings import CSV_KWARGS, FILE_FORMAT, NULL, OUTPUT_DIR
from .util import generate_rows, get_walked_obj
from .converters import CONVERTERS


class Extractor(object):
    """
    This handles the decoded json object and readies it for load into the db.
    """

    def __init__(
            self,
            name,
            schema_config,
            logger,
            local_db=None):
        self.name = name
        self.schema_config = schema_config
        self.logger = logger
        self._local_db = local_db
        self._flush_time = None
        self._init_local_db()

    @property
    def flush_time(self):
        """
        Returns a string representation of the time of the flushing.
        """
        if self._flush_time is None:
            self.flush_time = datetime.now()

        return self._flush_time

    @flush_time.setter
    def flush_time(self, value):
        self._flush_time = value

    @property
    def local_db(self):
        """
        Returns the local db handle.
        Also will initialize the db tables on first call.
        """
        if self._local_db is None:
            self._local_db = \
                sqlite3.connect(
                    os.path.join(OUTPUT_DIR, '.'.join([self.name, 'db'])),
                )
            if sys.version_info < (3, 0):
                self._local_db.text_factory = str

        return self._local_db

    def flush_table_data(self):
        """
        Write out csv files for the loader to load.
        And free the local db space.
        """
        self.reset_flush_time()
        for table_name in self.schema_config.configured_tables:
            payload_count = self.get_payload_count(table_name)
            if payload_count:
                self.logger.info(
                    "%s payloads of %s data ready to be exported...",
                    payload_count, table_name)

                # If exported successfully then flush data from the local_db
                if self.export_data_to_file(table_name):
                    self.reset_db_for_table(table_name)

    def reset_db_for_table(self, table_name):
        """
        Removes all of the successfully exported data from the local_db.
        """
        self.local_db.execute("DELETE FROM %s" % table_name)
        self.local_db.execute("VACUUM")
        self.local_db.commit()

    def export_data_to_file(self, table_name):
        """
        Return true if data written successfully else return false.
        """
        output_file = self.get_output_file(table_name)
        part_file = '.'.join([output_file, 'part'])
        try:
            row_count = 0
            with open(part_file, 'w') as partfile:
                writer = csv.writer(partfile, **CSV_KWARGS)
                query = """
                    SELECT payload
                      FROM %s
                     WHERE payload NOT NULL""" % table_name
                for rows, in self.local_db.execute(query):
                    for row in msgpack.unpackb(rows, encoding='utf-8'):
                        row_count += 1
                        writer.writerow(
                            [col if col is not None and col != ''
                             else NULL for col in row])
            os.rename(part_file, output_file)
            self.logger.info(
                "Wrote %s rows of data to %s", row_count, output_file)
            return True
        except OSError as exc:
            self.logger.error(exc)
            return False
        except Exception as exc:
            self.logger.error('UNHANDLED ERROR:%s;', exc)
            return False
        finally:
            if os.path.exists(part_file):
                os.remove(part_file)

    def get_current_revision(self, table_name, obj):
        """
        Queries a table for the revision by id and returns that revision or -1
        """
        query = "SELECT revision from %s WHERE id=?" % table_name
        for row in self.local_db.execute(query, (obj.get('id'),)):
            return row[0]

        return -1

    def get_output_file(self, table_name):
        """
        Return the output file for a given table_name.
        """
        outdir = \
            os.path.join(
                OUTPUT_DIR, self.name, table_name,
                self.flush_time.strftime('%Y/%m/%d/%H'))
        if not os.path.exists(outdir):
            os.makedirs(outdir)

        # Add new version if file already exists to ensure uniqueness
        for version in range(10000):
            items = [self.flush_time.strftime('%M%S'), FILE_FORMAT]
            if version > 0:
                items = \
                    [self.flush_time.strftime('%M%S'),
                     str(version), FILE_FORMAT]
            output_file = os.path.join(outdir, '.'.join(items))
            if not os.path.exists(output_file):
                return output_file

    def get_payload_count(self, table_name):
        """
        Get the payload count from the src_db.
        Each payload is a list of one or more rows.
        """
        return \
            self.local_db.execute(
                "SELECT count(*) FROM %s" % table_name).fetchone()[0]

    def persist(self, table_name, obj, payload):
        """
        Store to the local_db.
        """
        try:
            if bool(payload):
                self.local_db.execute(
                    'DELETE FROM %s WHERE id=?' % table_name, (obj.get('id'),))
                self.local_db.execute(
                    '''INSERT INTO %s (id, uid, revision, payload)
                       VALUES(?,?,?,?)''' % table_name,
                    (obj.get('id'), obj.get('uid'), obj.get('revision'),
                     msgpack.packb(payload)))
        except sqlite3.Error as error:
            self.logger.exception(error)

        return self.local_db.commit()

    def reset_flush_time(self):
        """
        Undefined the flush time so that it will be generated again on
         next call.
        """
        self.flush_time = None

    def update_handler(self, body_obj):
        """
        Entry point with the decoded json object.
        Here we decide on wether or not to process and keep the obj.
        """
        obj = body_obj.get('object')
        if int(obj.get('id')) < 0:
            return

        if 'updated_timestamp' not in obj:
            obj['updated_timestamp'] = \
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for table_name in self.schema_config.get_obj_tables(obj):
            local_revision = \
                int(self.get_current_revision(table_name, obj))
            if local_revision > int(obj.get('revision')):
                self.logger.warning(
                    "LOCAL_REVISION > THIS_REVISION: Skipping TYPE=%s, ID=%s, "
                    "UID='%s', REV=%s LOC_REV=(%s)",
                    table_name, obj.get('id'), obj.get('uid'),
                    obj.get('revision'), local_revision)
                continue
            rows = \
                generate_rows(
                    get_walked_obj(obj),
                    self.schema_config.get_column_names(table_name))
            if table_name in CONVERTERS:
                rows = CONVERTERS.get(table_name)(rows)
            self.persist(table_name, obj, list(rows))

    def _init_local_db(self):
        """
        Create all of the tables defined in the csv schema.
        """
        for table_name in self.schema_config.configured_tables:
            self.local_db.execute("""
                CREATE TABLE IF NOT EXISTS %s(
                    id INT PRIMARY KEY NOT NULL,
                    uid TEXT NOT NULL,
                    revision INT NOT NULL,
                    payload BINARY)""" % table_name)
        self.local_db.commit()
