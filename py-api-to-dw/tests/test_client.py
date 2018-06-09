import os
import unittest
from ox_dw_snowflake_api_to_dw import get_api_client, APIExtractorWorker, ignore_warnings

HERE = \
    os.path.join(
        os.path.abspath(
            os.path.join(os.path.dirname(__file__))))
CONSUMER_CONFIG_FILE = os.path.join(HERE, 'test-consumer-config.yaml')
SCHEMA_CONFIG_FILE = \
    os.path.join(HERE, 'test-api-extractor-schema-config.yaml')


class TestClient(unittest.TestCase):
    @ignore_warnings
    def setUp(self):
        self.client = get_api_client(CONSUMER_CONFIG_FILE, SCHEMA_CONFIG_FILE)

    def test_01_worker(self):
        self.assertTrue(isinstance(self.client.worker, APIExtractorWorker))


if __name__ == '__main__':
    unittest.main()
