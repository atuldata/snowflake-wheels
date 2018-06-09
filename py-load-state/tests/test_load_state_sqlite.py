import unittest
from load_state_base import LoadStateBase, VARIABLE_NAME
import os
from ox_dw_load_state import LoadState
import sqlite3

HERE = \
    os.path.join(
        os.path.abspath(
            os.path.join(os.path.dirname(__file__))))
DB_FILE = os.path.join(HERE, 'test.db')


class TestLoadStateSQLITE(unittest.TestCase, LoadStateBase):
    def setUp(self):
        self.load_state = LoadState(sqlite3.connect(DB_FILE), variable_name=VARIABLE_NAME)

    def tearDown(self):
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)


if __name__ == '__main__':
    unittest.main()
