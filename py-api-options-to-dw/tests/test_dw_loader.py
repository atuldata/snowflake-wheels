import unittest

from ox_dw_snowflake_api_options_to_dw import DWLoader


CONFIG = [
    {'test_table_01': {
        'sources': ['test_source_01'],
        'keys': [{'key_01': 'value_01'}, {'key_02': 'value_02'}],
        'values': [{'key_01': 'value_01'}, {'key_02': 'value_02'}],
        'transformations': ['transformation_01']}},
    {'test_table_02': {
        'sources': ['test_source_02'],
        'keys': [{'key_01': 'value_01'}],
        'values': [{'key_01': 'value_01'}],
        'transformations': ['transformation_02']}}
]


class TestDWLoader(unittest.TestCase):

    def setUp(self):
        self.dw_loader = DWLoader(CONFIG)

    def test_config_ordering(self):
        self.assertEqual(
            list(self.dw_loader.config.keys()), self.dw_loader.tables)

    def test_config_values(self):
        self.assertEqual(
            self.dw_loader.config.get('test_table_02').get('sources'),
            ['test_source_02'])

    def test_tables(self):
        self.assertEqual(
            self.dw_loader.tables, ['test_table_01', 'test_table_02'])


if __name__ == '__main__':
    unittest.main()
