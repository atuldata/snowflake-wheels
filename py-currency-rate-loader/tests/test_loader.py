import os
import sys
import unittest
from mock import patch
from ox_dw_snowflake_currency_rate_loader import BASE_CURRENCY, END_DATE, Loader
from ox_dw_snowflake_currency_rate_loader.commands import main

HERE = \
    os.path.join(
        os.path.abspath(
            os.path.join(os.path.dirname(__file__))))
Loader.load_tmp_file = lambda self: True


class TestLoader(unittest.TestCase):

    def setUp(self):
        self.loader = Loader(END_DATE)

    def tearDown(self):
        if os.path.exists(self.loader.temp_file):
            os.remove(self.loader.temp_file)

    def test_dates(self):
        for date in self.loader.dates:
            self.assertTrue(date == END_DATE)

    def test_end_date(self):
        self.assertEqual(self.loader.end_date, END_DATE)

    def test_start_date(self):
        self.assertEqual(self.loader.start_date, END_DATE)

    def test_get_currencies(self):
        self.assertTrue(isinstance(self.loader.get_currencies(END_DATE), list))

    def test_load_tmp_file(self):
        self.assertTrue(self.loader.load_tmp_file())

    def test_command(self):
        with patch.object(
                sys, 'argv',
                ['-s=%s' % END_DATE.strftime('%Y-%m-%d'),
                 '-e=%s' % END_DATE.strftime('%Y-%m-%d')]):
            Loader.start_date = END_DATE
            Loader.get_currencies = lambda self, date: [BASE_CURRENCY]
            main()


if __name__ == '__main__':
    unittest.main()
