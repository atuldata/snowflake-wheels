from mock import Mock
import unittest
import os
import smtplib
import ox_dw_logger
from ox_dw_logger import (
    email_report,
    print_report
)
from ox_dw_logger.log_parser import NAME

print = Mock()
smtplib.SMTP = Mock()
HERE = \
    os.path.join(
        os.path.abspath(
            os.path.join(os.path.dirname(__file__))))

LOG_FILE = os.path.join(HERE, 'test_etl.log')
LOCAL_DB_FILE = os.path.join(ox_dw_logger.LOCAL_OUTPUT, '.'.join([NAME, 'db']))


class TestEtlLogParser(unittest.TestCase):

    def setUp(self):
        if os.path.exists(LOCAL_DB_FILE):
            os.remove(LOCAL_DB_FILE)

    def tearDown(self):
        if os.path.exists(LOCAL_DB_FILE):
            os.remove(LOCAL_DB_FILE)

    def test_print_report(self):
        self.assertEqual(print_report('ERROR', path=HERE), None)

    def test_email_report(self):
        self.assertEqual(email_report('ERROR', path=HERE), None)


if __name__ == '__main__':
    unittest.main()
