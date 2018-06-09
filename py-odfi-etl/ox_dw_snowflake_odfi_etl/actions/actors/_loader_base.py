"""
Common methods for both Downloading and Uploading.
"""
import os
from datetime import datetime
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
from dateutil.parser import parse as parse_date
from snowflake.connector import ProgrammingError
from ...common.settings import (CREATE_STATUS_TABLE, INSERT_STATUS_STMT,
                                STAGE_NAME)

GET_BY_SERIAL = """
SELECT *
FROM %s
WHERE job_name = ? AND
      odfi_feed_name = ? AND
      dataset_serial = ?
"""


class _LoaderBase(object):
    """
    Common methods for both Downloading and Uploading.
    """

    def __init__(self, job):
        self.job = job
        self.stage_name = STAGE_NAME

    @property
    def status_table(self):
        """
        The name of the status table to use.
        """
        raise NotImplementedError

    def add_status(self, dataset, start_time):
        """
        Add new record to the status table after a succesful run.
        :param dataset: The dataset object.
        :param start_time: datetime of when job was started.
        """
        if self.get_status(dataset) is None:
            cursor = self.job.dbh.cursor()
            args = (self.job.name, dataset.feed_name,
                    self.job.feed.default_args.get('v'), dataset.serial,
                    dataset.meta.get('readableInterval'), parse_date(
                        dataset.meta.get('startTimestamp')), parse_date(
                            dataset.meta.get('endTimestamp')),
                    dataset.meta.get('schema').get('@version'),
                    dataset.meta.get('@recordCount'),
                    dataset.meta.get('@dataSize'),
                    dataset.meta.get('@revision'), parse_date(
                        dataset.meta.get('dateCreated')), start_time,
                    datetime.utcnow())
            self.job.logger.debug("ADD_STATUS: %s, %s",
                                  INSERT_STATUS_STMT % self.status_table, args)
            cursor.execute(INSERT_STATUS_STMT % self.status_table, args)

    def add_empty_dataset(self, dataset):
        """
        In the case where there are no part files then add to the
        status table so that no uploads will be performed as they
        are not needed.
        """
        self.job.logger.warning(
            "DatasetSerial(%s) is EMPTY!!! for uri %s. Adding entry to the "
            "%s table.", dataset.serial, dataset.uri, self.status_table)
        dataset.clear_part_files()
        self.add_status(dataset, datetime.utcnow())

    def create_stage(self):
        """
        Create the staging area on SnowFlake.
        """
        try:
            cursor = self.job.dbh.cursor()
            cursor.execute("CREATE STAGE %s" % self.stage_name)
            self.job.logger.info("Created stage %s", self.stage_name)
        except ProgrammingError as error:
            if 'already exists' in str(error):
                pass
            else:
                raise

    def create_status_table(self):
        """
        Create the status table if it doesn't exist.
        """
        cursor = self.job.dbh.cursor()
        cursor.execute(CREATE_STATUS_TABLE % self.status_table)

    def get_since_serial(self):
        """
        Method for getting the since serial to start with.
        """
        raise NotImplementedError

    def get_status(self, dataset):
        """
        Returns status of the dataset if exists.
        """
        cursor = self.job.dbh.cursor()
        cursor.execute(GET_BY_SERIAL % self.status_table,
                       (self.job.name, self.job.feed.name, dataset.serial))
        fields = [field[0].lower() for field in cursor.description]
        for row in cursor:
            return OrderedDict(dict(list(zip(fields, row))))

    def is_staged(self, dataset):
        """
        Returns True if all of the partfiles are indeed staged.
        Currently no need to check datasize od md5 hash as they are wrong in
        SnowFlake.
        """
        query = "LIST @%s/%s" % (self.stage_name,
                                 self._get_stage_path(dataset))
        self.job.logger.debug("Query for stage:%s;", query)
        staged_part_files = set([
            os.path.join(dataset.download_dir, os.path.basename(name))
            for name, _, _, _ in self.job.dbh.cursor().execute(query)
        ])
        for part_file in dataset.get_part_files():
            if part_file.file_name not in staged_part_files:
                self.job.logger.warning(
                    "Not staged yet. %s(%s). ODFI Status is %s.",
                    self.job.feed_name, dataset.serial,
                    dataset.meta['@status'])
                return False

        return True

    @staticmethod
    def _get_stage_path(dataset):
        """
        Return the remote staging path for a dataset.
        """
        return os.path.join(dataset.feed_name, str(dataset.serial))
