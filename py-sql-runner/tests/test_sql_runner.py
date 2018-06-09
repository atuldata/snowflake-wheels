import os
import sys
try:
    from io import StringIO
except ImportError:
    from cStringIO import StringIO

import unittest
import yaml
from ox_dw_snowflake_sql_runner.settings import ENV
from ox_dw_snowflake_sql_runner import run
from ox_dw_snowflake_sql_runner.util import ignore_warnings

HERE = \
    os.path.join(
        os.path.abspath(
            os.path.join(os.path.dirname(__file__))))
CONFIG = yaml.load(open(os.path.join(HERE, 'sql_runner.yaml')))
CONFIG['DSN'] = ENV.get('SNOWFLAKE')
EXPECTED = ['COL1@COL2', '4@four', '5@five', '6@six']


class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio    # free up some memory
        sys.stdout = self._stdout


class TestSQLRunner(unittest.TestCase):

    @ignore_warnings
    def test_run(self):
        """
        This will call run and verify the output.
        """
        output = []
        with Capturing(output) as output:
            run(CONFIG)
        for index, row in enumerate(output):
            self.assertEqual(row, EXPECTED[index])


if __name__ == '__main__':
    unittest.main()
