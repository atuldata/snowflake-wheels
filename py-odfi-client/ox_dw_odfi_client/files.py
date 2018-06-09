"""
File containers.
"""
import os
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse
import hashlib
from collections import namedtuple
from .exceptions import DataSizeMismatchException, MD5MismatchException


class PartFile(
        namedtuple('PartFile', [
            'url', 'file_name', 'record_count', 'data_size', 'part_key',
            'index', 'compression_type', 'digest', 'name', 'serial'
        ])):
    """
    Represents a PartFile including methods for validation.
    """

    __slots__ = ()

    @property
    def is_correct_md5_sum(self):
        """
        Is the md5 sum correct?
        :return boolean: When True
        :raises MD5MismatchException:
        """
        md5 = hashlib.md5()
        with open(self.file_name, 'rb') as in_file:
            for data_chunk in iter(lambda: in_file.read(4096), b""):
                md5.update(data_chunk)

        if self.digest != md5.hexdigest():
            raise MD5MismatchException(self, md5.hexdigest())

        return True

    @property
    def is_correct_size(self):
        """
        Is the data_size what is expected?
        :return boolean: When True
        :raises DataSizeMismatchException:
        """
        if self.data_size != os.stat(self.file_name).st_size:
            raise \
                DataSizeMismatchException(
                    self, os.stat(self.file_name).st_size)

        return True

    @property
    def is_ready(self):
        """
        Is file ready for upload?
        Only checks if url protocol in (http, https, ftp)
        :return boolean:
        """
        if urlparse(self.url).scheme.startswith(('http', 'ftp')):
            return all([self.is_correct_md5_sum, self.is_correct_size])
        else:
            return True


class SchemaFile(
        namedtuple('SchemaFile',
                   ['url', 'file_name', 'version', 'name', 'serial'])):
    """
    Represents a SchemaFile including a method for validation.
    """

    __slots__ = ()

    @property
    def is_ready(self):
        """
        Is the schema file ready for use?
        :return boolean:
        """
        return os.path.isfile(self.file_name)
