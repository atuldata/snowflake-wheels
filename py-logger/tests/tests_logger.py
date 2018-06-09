import logging
import unittest
import os

from ox_dw_logger import get_etl_logger, LOG_DIRECTORY

SIMPLE_LOG_NAME = 'SimpleLogger'
NEW_LOG_NAME = 'NewLogger'
NEW_LOG_DIR = 'tests'
NEW_LOG_LEVEL = 'DEBUG'
NEW_LOG_FORMAT = '[%(asctime)s] ' + \
                 '[%(module)s:%(name)s.%(funcName)s:%(lineno)d] ' + \
                 '[%(levelname)s] %(message)s'


class TestEtlLoggerNewWithOptions(unittest.TestCase):
    def setUp(self):
        self.logger = \
            get_etl_logger(
                log_name=NEW_LOG_NAME, log_directory=NEW_LOG_DIR,
                log_format=NEW_LOG_FORMAT, log_level=NEW_LOG_LEVEL)

    def tearDown(self):
        if os.path.exists(self.logger.handlers[0].baseFilename):
            os.remove(self.logger.handlers[0].baseFilename)

    def test_log_name(self):
        self.assertEqual(self.logger.name, NEW_LOG_NAME)

    def test_log_directory(self):
        self.assertEqual(
            self.logger.handlers[0].baseFilename,
            os.path.join(os.path.abspath(os.path.dirname(__file__)),
                         '.'.join([NEW_LOG_NAME, 'log'])))

    def test_log_same_name(self):
        logger = get_etl_logger(log_name=NEW_LOG_NAME)
        self.assertEqual(
            self.logger.handlers[0].baseFilename,
            logger.handlers[0].baseFilename
        )

    def test_log_format(self):
        self.assertEqual(
            self.logger.handlers[0].formatter._fmt, NEW_LOG_FORMAT)

    def test_log_level(self):
        self.assertEqual(self.logger.level, getattr(logging, NEW_LOG_LEVEL))

    def test_None_log_directory(self):
        logger = get_etl_logger(log_name='test', log_directory=None,
                                log_format=None, log_level=NEW_LOG_LEVEL)
        self.assertEqual(
            logger.handlers[0].baseFilename,
            os.path.join(os.path.abspath(LOG_DIRECTORY),
                         '.'.join(['test', 'log']))
        )


if __name__ == '__main__':
    unittest.main()
