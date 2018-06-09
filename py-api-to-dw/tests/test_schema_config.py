import os
import unittest
from ox_dw_snowflake_api_to_dw import SchemaConfig, ignore_warnings

KNOWN_ACTION_TYPES = set(['action_type_01', 'action_type_02'])
KNOWN_CONFIGURED_TABLES = \
    set(['any_match', 'any_type_match', 'type_01_match', 'type_02_match',
         'type_02_full_match', 'type_03_match', 'test_data'])
KNOWN_COLUMNS = tuple(['id', 'col_01', 'col_02'])
OBJECTS = [
    {"type": "type_01",
     "destinations": set(["type_01_match", "test_data"]),
     "description": "Matches 1 key:value pair."},
    {"type": "type_02",
     "destinations": set(["type_02_match"]),
     "description": "Matches 1 key:value pair."},
    {"type": "type_02",
     "type_full": "type_02_full",
     "destinations": set(["type_02_match", "type_02_full_match"]),
     "description": "Matches 2 key:value pairs."},
    {"type": "type_03",
     "my_list": [1, 2, 3],
     "destinations": set(["type_03_match"]),
     "description": "Matches 1 key:value pair AND list is not empty."},
    {"type": "type_04",
     "my_list": ['item0', 'item1', 'item2'],
     "destinations": set(["type_04_match", "type_04_0_match"]),
     "description": \
         "Matches 1 key:value pair also matches list items by index."}
]

HERE = \
    os.path.join(
        os.path.abspath(
            os.path.join(os.path.dirname(__file__))))
CONFIG_FILE = os.path.join(HERE, 'test-api-extractor-schema-config.yaml')


class TestSchemaConfig(unittest.TestCase):
    @ignore_warnings
    def setUp(self):
        self.config = SchemaConfig(CONFIG_FILE)

    def test_config_file(self):
        self.assertEqual(CONFIG_FILE, self.config.schema_config_file)

    def test_action_types(self):
        self.assertEqual(
            len(set(self.config.action_types).intersection(KNOWN_ACTION_TYPES)),
            len(KNOWN_ACTION_TYPES.intersection(set(self.config.action_types)))
        )

    def test_configured_tables(self):
        self.assertEqual(len(self.config.configured_tables), 3)

    def test_configured_tables(self):
        self.assertEqual(
            len(self.config.configured_tables.intersection(
                KNOWN_CONFIGURED_TABLES)),
            len(KNOWN_CONFIGURED_TABLES.intersection(
                self.config.configured_tables))
        )

    def test_column_names(self):
        self.assertEqual(
            self.config.get_column_names('any_match'), KNOWN_COLUMNS)

    def test_get_obj_tables(self):
        for obj in OBJECTS:
            match_set = set([
                table_name for table_name in self.config.get_obj_tables(obj)])
            self.assertEqual(
                match_set, obj['destinations'], msg=obj['description'])


if __name__ == '__main__':
    unittest.main()
