import unittest
from load_state_base import LoadStateBase, VARIABLE_NAME
from ox_dw_db import OXDB
from ox_dw_db.settings import ENV
from ox_dw_load_state import LoadState
from ox_dw_load_state.util import ignore_warnings
DB_CONNECTION_NAME = 'SNOWFLAKE'


class TestLoadStateODBC(unittest.TestCase, LoadStateBase):
    @ignore_warnings
    def setUp(self):
        self.load_state = LoadState(OXDB(DB_CONNECTION_NAME).connection, variable_name=VARIABLE_NAME)

    def tearDown(self):
        self.load_state.delete()


if __name__ == '__main__':
    unittest.main()
