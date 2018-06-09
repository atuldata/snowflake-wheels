from mock import Mock
import unittest

import ox_dw_db
import ox_dw_logger
import ox_dw_load_state
from ox_dw_snowflake_api_options_to_dw import DWLoader, TableLoader

ox_dw_db.OXDB = Mock()
ox_dw_load_state.LoadState = Mock()
ox_dw_logger.get_etl_logger = Mock()

CONFIG = [
    {'test_table_01': {
        'sources': ['test_source_01'],
        'keys': [{'key_01': 'value_01'}, {'key_02': 'value_02'}],
        'values': [{'key_01': 'value_01'}, {'key_02': 'value_02'}],
        'key_sep': '@',
        'transformations': ['transformation_01']}},
    {'test_table_02': {
        'sources': ['test_source_02'],
        'keys': [{'key_01': 'value_01'}],
        'values': [{'key_01': 'value_01'}],
        'transformations': ['transformation_02']}}
]


class TestTableLoader(unittest.TestCase):

    def setUp(self):
        dw_loader = DWLoader(CONFIG)
        self.table_loader = \
            TableLoader(
                dw_loader.tables[0], dw_loader.config.get(dw_loader.tables[0]))

    def test_source_fields(self):
        self.assertEqual(
            self.table_loader.source_fields,
            ['key_01', 'key_02', 'key_01', 'key_02'])

    def test_destination_fields(self):
        self.assertEqual(
            self.table_loader.destination_fields,
            ['value_01', 'value_02', 'value_01', 'value_02'])

    def test_key_sep(self):
        self.assertEqual(self.table_loader.key_sep, '@')

    def test_kwargs(self):
        self.assertEqual(
            self.table_loader.kwargs.get('temp_table'), 'tmp_test_table_01')


if __name__ == '__main__':
    unittest.main()
