from mock import Mock
import os
import unittest

import ox_dw_load_state
import ox_dw_logger

ox_dw_load_state.LoadState = Mock()
ox_dw_logger.get_etl_logger = Mock()
from ox_dw_snowflake_api_to_dw import (
   SchemaConfig, TableLoader,
   ignore_warnings
)

HERE = \
    os.path.join(
        os.path.abspath(
            os.path.join(os.path.dirname(__file__))))
SCHEMA_CONFIG_FILE = \
    os.path.join(HERE, 'test-api-extractor-schema-config.yaml')
SCHEMA_CONFIG = SchemaConfig(SCHEMA_CONFIG_FILE)
TABLE_NAME = 'test_data'
DATA_FILE = os.path.join(HERE, '%s.csv' % TABLE_NAME)
CONFIG_FILE = os.path.join(HERE, '%s.yaml' % TABLE_NAME)


class TestTableLoader(unittest.TestCase):
    @ignore_warnings
    def setUp(self):
        self.table_loader = \
            TableLoader(
                TABLE_NAME,
                DATA_FILE,
                SCHEMA_CONFIG,
                config_file=CONFIG_FILE,
                dest_db=Mock(schema=TABLE_NAME, get_executed_cursor=lambda x: []))
        setattr(self.table_loader, 'archive_outfile', Mock())
        setattr(self.table_loader, 'sync_table', Mock())

    def test_01(self):
        self.table_loader.main()


if __name__ == '__main__':
    unittest.main()
