import unittest

from ox_dw_db.oxdb import OXDB
from ox_dw_db.util import ignore_warnings

SNOWFLAKE_CONNECTION_NAME = 'SNOWFLAKE'


class TestSnowflakeOXDB(unittest.TestCase):
    @ignore_warnings
    def setUp(self):
        self.dbh = OXDB(SNOWFLAKE_CONNECTION_NAME)

    def tearDown(self):
        self.dbh.close()

    def test_execute_no_args(self):
        self.assertEqual(
            self.dbh.execute("select 1"), 1,
            'Rows effected should be one only.')

    def test_execute_sql_args(self):
        self.assertEqual(
            self.dbh.execute("select 1 = ?", (1,)), 1)
        self.assertNotEqual(
            self.dbh.execute("select 1 = ?", (1,)), 0)
        self.assertEqual(
            self.dbh.execute("select 1 = ?, 2 = ?", (1, 2)), 1)

    def test_commit(self):
        with OXDB(SNOWFLAKE_CONNECTION_NAME) as oxdb:
            oxdb.execute("select 1 = ?", (1,))


if __name__ == '__main__':
    unittest.main()
