import pyodbc
import unittest

from ox_dw_db.oxdb import OXDB

IMPALA_CONNECTION_NAME = 'IMPALA'


class TestImpalaOXDB(unittest.TestCase):
    def setUp(self):
        self.dbh = OXDB(IMPALA_CONNECTION_NAME, transactions_support=False)

    def tearDown(self):
        self.dbh.close()

    def test_connection_instance(self):
        self.assertTrue(isinstance(self.dbh.connection, pyodbc.Connection))

    def test_execute_no_args(self):
        self.assertEqual(
            len(self.dbh.get_executed_cursor("select 1").fetchall()), 1,
            'Rows effected should be one only.')

    def test_execute_sql_args(self):
        first_val = 1
        second_val = 2
        self.assertEqual(
            len(self.dbh.get_executed_cursor(
                "select 1 = %s" % first_val).fetchall()), 1)
        self.assertNotEqual(
            len(self.dbh.get_executed_cursor(
                "select 1 = %s" % first_val).fetchall()), 0)
        self.assertEqual(
            len(self.dbh.get_executed_cursor(
                "select 1 = %s, 2 = %s" % (first_val, second_val)).fetchall()), 1)

    def test_commit(self):
        with OXDB(IMPALA_CONNECTION_NAME, transactions_support=False) as oxdb:
            oxdb.execute("select 1 = 1")


if __name__ == '__main__':
    unittest.main()
