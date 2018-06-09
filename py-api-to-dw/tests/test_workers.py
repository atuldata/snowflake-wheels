from datetime import datetime, timedelta
import logging
from mock import Mock
import os
import unittest

from ox_dw_snowflake_api_to_dw import APIExtractorWorker, SchemaConfig, FLUSH_PERIOD

HERE = \
    os.path.join(
        os.path.abspath(
            os.path.join(os.path.dirname(__file__))))
NAME = 'test-workers'
SCHEMA_CONFIG_FILE = \
    os.path.join(HERE, 'test-api-extractor-schema-config.yaml')
SCHEMA_CONFIG = SchemaConfig(SCHEMA_CONFIG_FILE)
TEST_OBJ = {
    'object': {
        'type': 'test_obj',
        'id': 12345,
        'uid': 'uid text',
        'action': SCHEMA_CONFIG.action_types[0]
    }
}


class TestWorkers(unittest.TestCase):
    def setUp(self):
        self.worker = \
            APIExtractorWorker(
                NAME, SCHEMA_CONFIG, logging.getLogger(NAME)
            )
        self.worker._extractor = Mock()

    def test_is_flush_time(self):
        self.assertFalse(self.worker.is_flush_time())

    def test_close(self):
        self.worker.close()

    def test_on_timeout(self):
        self.worker.last_flush_time = datetime.now() - timedelta(
            minutes=FLUSH_PERIOD)
        self.assertTrue(self.worker.on_timeout() is None)

    def test_process(self):
        self.assertEqual(self.worker.process(TEST_OBJ), {})

    def test_reset_flush_time(self):
        last_flush_time_01 = self.worker.last_flush_time
        self.worker.reset_flush_time()
        last_flush_time_02 = self.worker.last_flush_time
        self.assertNotEqual(last_flush_time_01, last_flush_time_02)


if __name__ == '__main__':
    unittest.main()
