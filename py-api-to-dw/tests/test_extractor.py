import os
import shutil
import unittest

import ujson as json

from ox_dw_snowflake_api_to_dw import (
    Extractor,
    SchemaConfig,
    OUTPUT_DIR,
    ignore_warnings
)
from ox_dw_logger import get_etl_logger

HERE = \
    os.path.join(
        os.path.abspath(
            os.path.join(os.path.dirname(__file__))))
JSON_FILE = os.path.join(HERE, 'test_data.json')
SCHEMA_CONFIG_FILE = \
    os.path.join(HERE, 'test-api-extractor-schema-config.yaml')
NAME = 'test_extractor'


class TestExtractor(unittest.TestCase):
    @ignore_warnings
    def setUp(self):
        print('')
        self.extractor = \
            Extractor(
                NAME, SchemaConfig(SCHEMA_CONFIG_FILE),
                get_etl_logger(NAME))
        self.table_name = os.path.splitext(os.path.basename(JSON_FILE))[0]
        with open(JSON_FILE, 'r') as in_json:
            for raw in in_json:
                self.extractor.update_handler(json.loads(raw))

    def tearDown(self):
        output_dir = os.path.join(OUTPUT_DIR, self.extractor.name)
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        local_db = \
            os.path.join(OUTPUT_DIR, '.'.join([self.extractor.name, 'db']))
        if os.path.exists(local_db):
            os.remove(local_db)

    def test_01(self):
        out_file = self.extractor.get_output_file(self.table_name)
        self.extractor.flush_table_data()
        self.assertEqual(4, sum(1 for line in open(out_file, 'r')))

    def test_output_file_uniqueness(self):
        """
        Run 10,000 times to try to get a file name collision.
        """
        output_files = set()
        for index in range(10000, 1):
            output_files.add(self.extractor.get_output_file(self.table_name))
            self.assertEqual(index, len(output_files))


if __name__ == '__main__':
    unittest.main()
