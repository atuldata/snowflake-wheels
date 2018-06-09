import os
from datetime import datetime
from shutil import rmtree
import unittest
from ox_dw_odfi_client import (
    DataSet, DataSizeMismatchException, MD5MismatchException, MissingMetaFile,
    NoDataSetException, Feed, Feeds, SchemaFile, download_file, get_xml_meta,
    readable_interval_str, get_interval_format)

HERE = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__))))
CONFIG = {
    'ODFI_HOST': 'http://qa-odfi-data-api.xv.dc.openx.org',
    'ODFI_USER': 'qa-odfi-dm',
    'ODFI_PASS': 'V5JQXuZ974j766X',
    'DATA_DIR': os.path.join(HERE, 'data'),
    'MAX_DATASETS': 100,
    'CACHE_META_DATA': True
}
LIVE_FEED = 'TrafficReport'
NOT_READY = 'NotReady'
BAD_DIGEST = 'BadDigest'
BAD_DATA_SIZE = 'BadDataSize'
TEST_SERIAL = '1234'
SAMPLE_XML = os.path.join(HERE, 'sample.xml')
SAMPLE_XML_URL = \
    CONFIG["ODFI_HOST"] + "/datasets/schema/AdaptiveInsightsDataReport,1.xml"
SAMPLE_SCHEMA_FILE = SchemaFile(SAMPLE_XML_URL, SAMPLE_XML, '1', 'sample_xml',
                                TEST_SERIAL)


class TestFeeds(unittest.TestCase):
    def setUp(self):
        self.feeds = Feeds(CONFIG)

    def tearDown(self):
        fresh_dir = os.path.join(CONFIG['DATA_DIR'], LIVE_FEED)
        if os.path.exists(fresh_dir):
            rmtree(fresh_dir)
        if os.path.exists(SAMPLE_XML):
            os.remove(SAMPLE_XML)

    def test_attempt_download_fresh(self):
        """
        Download a file from a live feed.
        """
        self.assertTrue(
            self.feeds[LIVE_FEED].latest_dataset.attempt_download())

    def test_dataset_not_ready(self):
        """
        Recognize when not ready.
        """
        self.assertFalse(self.feeds[NOT_READY].get_dataset_by_serial(
            TEST_SERIAL).is_ready_to_download)

    def test_dataset_bad_digest(self):
        """
        Recognize bad digest.
        """
        try:
            is_ready = \
                self.feeds[BAD_DIGEST].get_dataset_by_serial(
                    TEST_SERIAL).is_download_complete
            self.assertFalse(is_ready)
        except MD5MismatchException:
            pass

    def test_dataset_bad_data_size(self):
        """
        Recognize bad file size.
        """
        try:
            is_ready = \
                self.feeds[BAD_DATA_SIZE].get_dataset_by_serial(
                    TEST_SERIAL).is_download_complete
            self.assertFalse(is_ready)
        except DataSizeMismatchException:
            pass

    def test_missing_meta_file(self):
        """
        Test MissingMetaFile exception.
        """
        try:
            raise MissingMetaFile('where_is_my_meta?')
        except MissingMetaFile as error:
            self.assertTrue(isinstance(error, MissingMetaFile))

    def test_02_get_datasets_no_results(self):
        try:
            for dataset in \
                    self.feeds[LIVE_FEED].get_dataset_by_serial('5000000000'):
                self.assertTrue(isinstance(dataset, DataSet))
            self.fail()
        except NoDataSetException:
            pass
        else:
            self.fail()

    def test_get_since_serial_list(self):
        serial = str(int(self.feeds[LIVE_FEED].latest_dataset.serial) - 2)
        for dataset in self.feeds[LIVE_FEED].get_datasets_since_serial(serial):
            self.assertTrue(isinstance(dataset, DataSet))

    def test_feeds_iter(self):
        for feed in self.feeds:
            self.assertTrue(isinstance(feed, Feed))

    def test_get_xml_meta_malformed_xml(self):
        with open(os.path.join(HERE, 'meta.xml'), 'w') as out_file:
            out_file.write('HELLO')
        try:
            get_xml_meta(CONFIG, HERE)
            self.fail()
        except MissingMetaFile:
            pass

    def test_download_file(self):
        self.assertTrue(download_file(CONFIG, SAMPLE_SCHEMA_FILE))

    def test_get_since_serial_single(self):
        """
        Makes sure to convert and non-list to a list since XML is not
        consistent.
        """
        serial = str(int(self.feeds[LIVE_FEED].latest_dataset.serial) - 1)
        for dataset in self.feeds[LIVE_FEED].get_datasets_since_serial(serial):
            self.assertTrue(isinstance(dataset, DataSet))

    def test_01_get_datasets_no_results(self):
        """
        This should test first. Tests for results outside the range of existing
        serials.
        """
        serial = self.feeds[LIVE_FEED].latest_dataset.serial
        try:
            for dataset in \
                    self.feeds[LIVE_FEED].get_datasets_since_serial(serial):
                self.assertTrue(isinstance(dataset, DataSet))
            self.fail()
        except NoDataSetException:
            pass
        else:
            self.fail()

    def test_feed_meta(self):
        """
        Check a value in the meta of the feed in 3 methods to get.
        """
        self.assertTrue(self.feeds[NOT_READY].meta['frequency'] == 'HOURLY')
        self.assertTrue(
            self.feeds.get(NOT_READY).meta['frequency'] == 'HOURLY')
        self.assertTrue(
            getattr(self.feeds, NOT_READY).meta['frequency'] == 'HOURLY')

    def test_readable_interval_str(self):
        """
        Verify that a datetime is returned properly formatted.
        """
        self.assertEqual(
            readable_interval_str(
                datetime(2018, 1, 1, 23),
                get_interval_format('2018-01-01_23')), '2018-01-01_23')

    def test_get_interval_format_good(self):
        """
        Verify known formats.
        """
        self.assertEqual(get_interval_format('2018-01-01_23'), "%Y-%m-%d_%H")
        self.assertEqual(get_interval_format('2018-01-01'), "%Y-%m-%d")

    def test_get_interval_format_bad(self):
        """
        Verify unknown format.
        """
        try:
            get_interval_format('July 8th 1970')
        except ValueError:
            pass


if __name__ == '__main__':
    unittest.main()
