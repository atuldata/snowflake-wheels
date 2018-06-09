"""
Downloads and caches metadata and part files.
"""
from datetime import datetime
from ox_dw_odfi_client import DataSizeMismatchException, MD5MismatchException
from ._loader_base import _LoaderBase
from ...common.exceptions import JobNotFoundException
from ...common.settings import (COMPRESSION, DOWNLOAD_STATUS_TABLE_NAME,
                                DOWNLOAD_SORT_KEY)

GET_SINCE_SERIAL = """
SELECT max(dataset_serial)
FROM %s
WHERE job_name = ? AND
      odfi_feed_name = ?
""" % DOWNLOAD_STATUS_TABLE_NAME


class Downloader(_LoaderBase):
    """
    For loading a single job into the DW.
    Call this class to run the main loader.
    Returns True if data was actually downloaded.
    Example:
    from ox_dw_snowflake_odfi_etl import Downloader
    has_datafiles = Downloader(job, logger)()
    if has_datafiles:
        do other work like rollups, etc...
    """

    def __init__(self, job):
        super(Downloader, self).__init__(job)

    def __call__(self, max_datasets=None, since_serial=None):
        """
        Iterate through the datasets and download.
        For datasets that are ready but have no data they will be recorded
        in the status table.
        :param max_datasets: This is optional, by default there is no max
        :param since_serial: Optional. Will use the load state serial value.
        """
        if since_serial is None:
            since_serial = self.get_since_serial()
        self.job.logger.debug("SINCE_SERIAL: %s;", since_serial)

        has_data = False
        for dataset in self.job.feed.get_datasets_since_serial(
                since_serial,
                max_datasets=max_datasets,
                sort_key=DOWNLOAD_SORT_KEY):

            if self.download_and_stage(dataset):
                has_data = True

            # Safe to commit after each dataset is completed.
            self.job.dbh.commit()

        return has_data

    @property
    def status_table(self):
        """
        The status table to use.
        """
        return DOWNLOAD_STATUS_TABLE_NAME

    def download(self, dataset):
        """
        Download from ODFI to disk.
        """
        try:
            if dataset.is_download_complete:
                self.job.logger.debug(
                    "DatasetSerial(%s) is already downloaded and is "
                    "completely valid.", dataset.serial)
        except (IOError, DataSizeMismatchException, MD5MismatchException):
            self.job.logger.info(
                "START: DatasetSerial(%s) downloading from %s", dataset.serial,
                dataset.uri)
            if not dataset.attempt_download():
                return False
            self.job.logger.info(
                "FINISH: DatasetSerial(%s) downloaded from %s", dataset.serial,
                dataset.uri)

    def download_and_stage(self, dataset):
        """
        Download and Stage.
        """
        has_data = False
        start_time = datetime.utcnow()
        # Only download if it has part file data.
        if dataset.has_part_files:
            if not self.is_staged(dataset):
                self.download(dataset)
                self.stage(dataset)
                has_data = True
            self.add_status(dataset, start_time)
        else:
            self.add_empty_dataset(dataset)

        # Safe to commit after each dataset is completed.
        self.job.dbh.commit()

        return has_data

    def download_by_serial_range(self, start_serial, end_serial):
        """
        Similar to the __call__ but for a specific range of serials.
        """
        if end_serial is None:
            end_serial = self.job.feed.latest_dataset.serial

        for dataset_serial in range(start_serial, end_serial + 1):
            self.download_and_stage(
                self.job.feed.get_dataset_by_serial(dataset_serial))

    def get_since_serial(self):
        """
        Returns the since serial to use for querying ODFI.
        """
        self.job.logger.debug("get_serial(): %s; %s", GET_SINCE_SERIAL,
                              (self.job.name, self.job.feed.name))
        for dataset_serial, in self.job.dbh.cursor().execute(
                GET_SINCE_SERIAL, (self.job.name, self.job.feed.name)):
            if dataset_serial is None:
                raise JobNotFoundException(self.job.name)
            return dataset_serial

    def stage(self, dataset):
        """
        Put the dataset files up to the staging area. Can only be called if
        the files are downloaded.
        """
        put_stmt = """
            PUT file://%s/*.txt.gz
            @%s/%s
            PARALLEL = 15
            AUTO_COMPRESS = False
            SOURCE_COMPRESSION = %s""" % (dataset.download_dir,
                                          self.stage_name,
                                          self._get_stage_path(dataset),
                                          COMPRESSION)
        self.job.logger.debug("PUT_STMT:%s;", put_stmt)
        self.job.dbh.cursor().execute(put_stmt)
        dataset.clear_part_files()
