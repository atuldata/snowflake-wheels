"""
This is where most of the work occurs for reading from ODFI and downloading
files.
"""
import os
from multiprocessing import Pool
from shutil import rmtree
from .download import download_file, get_download_dir, get_xml_meta
from .exceptions import DataSizeMismatchException, MD5MismatchException
from .files import PartFile, SchemaFile
from .readable_interval import (get_interval, get_next_readable_interval,
                                readable_interval_datetime)
from .settings import CACHE_META_DATA, MAX_DOWNLOADERS


class DataSet(object):
    """
    This is an object representing a single feeds datasets.
    """

    def __init__(self, config, feed_name, serial, uri=None):
        self.config = config
        self.feed_name = feed_name
        self.serial = serial
        self.uri = uri
        self.download_dir = get_download_dir(
            self.config, feed_name=self.feed_name, serial=self.serial)
        self.meta = get_xml_meta(
            self.config,
            get_download_dir(
                self.config, feed_name=self.feed_name, serial=self.serial),
            name='meta.xml',
            url=self.uri).get('dataset')
        self.schema = get_xml_meta(
            self.config,
            get_download_dir(
                self.config, feed_name=self.feed_name, serial=self.serial),
            name='schema.xml',
            url=self.meta['schema']['@locator']).get('partition_schema')
        self.readable_interval = readable_interval_datetime(
            self.meta['readableInterval'])
        self.next_readable_interval = get_next_readable_interval(
            readable_interval_datetime(self.meta['readableInterval']),
            get_interval(self.meta['readableInterval']))

    @property
    def has_part_files(self):
        """
        Are there any part files?
        """
        return 'part' in self.meta['parts']

    @property
    def is_download_complete(self):
        """
        Will check that all files exist and are ready.
        :return boolean:
        :raises IOError, DataSizeMismatchException, MD5MismatchException:
        """
        return all([download.is_ready for download in self.get_files()])

    @property
    def is_ready_to_download(self):
        """
        Is ready to download?
        :return boolean:
        """
        return self.meta['@status'] == 'READY'

    def attempt_download(self):
        """
        Download the part files and schema file.
        :return boolean:
        :raises IOError, RetryError, DataSizeMismatchException,
                MD5MismatchException:
        """
        if self.is_ready_to_download:
            try:
                return self.is_download_complete
            except (IOError, DataSizeMismatchException, MD5MismatchException):
                if not os.path.exists(self.download_dir):
                    os.makedirs(self.download_dir)
                pool = Pool(processes=self.config.get('MAX_DOWNLOADERS',
                                                      MAX_DOWNLOADERS))
                for file_obj in self.get_files():
                    pool.apply_async(
                        download_file, args=[self.config, file_obj])
                pool.close()
                pool.join()
                return self.is_download_complete
        else:
            return False

    def clear_part_files(self):
        """
        Remove only the downloaded datasets.
        """
        for part_file in self.get_part_files():
            if os.path.exists(part_file.file_name):
                os.remove(part_file.file_name)

    def clear_download(self):
        """
        Remove the folder for the dataset/serial and all of it's contents.
        """
        if os.path.exists(self.download_dir):
            rmtree(self.download_dir)

    def get_files(self):
        """
        :return generator: PartFiles and SchemaFile
        """
        for part_file in self.get_part_files():
            yield part_file

        if self.config.get('CACHE_META_DATA', CACHE_META_DATA):
            yield self.get_schema_file()

    def get_part_files(self):
        """
        :return generator: PartFiles
        """
        if self.has_part_files:
            parts = self.meta['parts']['part']
            if not isinstance(parts, list):
                parts = [parts]
            for part in parts:
                yield PartFile(
                    part['@locator'],
                    os.path.join(self.download_dir,
                                 part.get('part_key', '000') + '-' +
                                 part['@index'].zfill(3) + '.txt.gz'),
                    int(part['@recordCount']),
                    int(part['@dataSize']),
                    part.get('part_key', '000'),
                    int(part['@index']), part['@compressionType'],
                    part['@digest'], self.feed_name, self.serial)

    def get_schema_file(self):
        """
        :return SchemaFile:
        """
        return SchemaFile(self.meta['schema']['@locator'],
                          os.path.join(self.download_dir, 'schema.xml'),
                          int(self.meta['schema']['@version']),
                          self.meta['schema']['@name'], self.serial)
