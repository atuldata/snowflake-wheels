"""
With this Feed object you can work with data sets.
"""
import os
from operator import itemgetter
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
from .dataset import DataSet
from .download import get_xml, get_xml_meta, get_download_dir
from .exceptions import MissingMetaFile, NoDataSetException
from .settings import QUERY_REST_PATH, DEFAULT_ODFI_VERSION, DEFAULT_SORT_KEY


class Feed(object):
    """
    This is an object representing a single feed.
    Additional attributes are added from the xml meta from ODFI for this feed.
    """

    def __init__(self, config, name, uri):
        self.config = config
        self.name = name
        self.uri = uri
        self.query_url = os.path.join(config['ODFI_HOST'].rstrip(os.path.sep),
                                      QUERY_REST_PATH.strip(os.path.sep),
                                      self.name)
        self.default_args = {
            'v': str(self.config.get('ODFI_VERSION', DEFAULT_ODFI_VERSION))
        }
        if 'MAX_DATASETS' in self.config:
            self.default_args.update({
                'max':
                str(self.config.get('MAX_DATASETS'))
            })
        self.meta = get_xml_meta(
            self.config,
            get_download_dir(self.config, feed_name=self.name),
            name='meta.xml',
            url=self.uri).get('feed')

    @property
    def latest_dataset(self):
        """
        :return DataSet: The latest one for this feed.
        """
        return next(self.get_datasets(args={'interval': 'latest'}))

    def get_dataset_by_serial(self, serial):
        """
        This can only be used once the dataset has been downloaded.
        When you know the serial you need then call this one.
        :param str: Serial of the dataset
        :return Dataset:
        """
        try:
            return DataSet(self.config, self.name, serial)
        except MissingMetaFile:
            return next(
                self.get_datasets({
                    'since_serial': str(int(serial) - 1),
                    'max': 1
                }))

    def get_datasets(self, args=None, sort_key=DEFAULT_SORT_KEY):
        """
        Yields datasets using default_args updated with args.
        The datasets are returned by distinct startTimestamp.
        In the case of more than one serial for the same startTimestamp
        only the latest serial is returned.
        :param args: dict()
        :return generator: DataSet
        """
        doc = self._get_dataset_doc(args=args)
        if not isinstance(doc['dataset'], list):
            doc['dataset'] = [doc['dataset']]

        def _get_key(dataset):
            return getattr(dataset, sort_key)

        for dataset in sorted(
            [
                DataSet(self.config, self.name,
                        int(dataset['serial']), dataset['@uri'])
                for dataset in doc['dataset']
            ],
                key=_get_key):
            yield dataset

    def get_datasets_since_serial(self, serial, **args):
        """
        Yields datasets since given serial.
        :param serial: This is the minimum serial for the query against ODFI.
        :return generator: DataSet
        """
        kwargs = {
            'since_serial': serial,
            'max': args.get('max_datasets', self.config.get('MAX_DATASETS'))
        }
        sort_key = args.get('sort_key', DEFAULT_SORT_KEY)
        for dataset in self.get_datasets(args=kwargs, sort_key=sort_key):
            yield dataset

    def get_min_dataset(self, readable_interval_str):
        """
        Return the minimum dataset needed to cover all readable_intervals
        from the readable_interval_str supplied.
        :param readable_interval_str: String representaion of a
                                      readable_interval.
        :returns DataSet:
        """
        return self.get_dataset_by_serial(
            min(
                int(doc.get('serial'))
                for doc in self._get_dataset_doc({
                    'start': readable_interval_str
                }).get('dataset', {}) if doc.get('serial') is not None) - 1)

    def _get_dataset_doc(self, args=None):
        """
        :param args: dict
        :return dict: Document of datasets
        """
        params = dict(self.default_args)
        if args is not None:
            params.update(args)
        doc = get_xml(self.config, self.query_url, params=params)['datasets']
        if 'dataset' in doc:
            return doc
        else:
            raise NoDataSetException(self.query_url, params)
