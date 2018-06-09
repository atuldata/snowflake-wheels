import os
import unittest
from ox_dw_content_topic_loader import ENV
from ox_dw_db import OXDB
from ox_dw_content_topic_loader import Loader
from ox_dw_snowflake_content_topic_loader.util import ignore_warnings

HERE = \
    os.path.join(
        os.path.abspath(
            os.path.join(os.path.dirname(__file__))))
ENV['CUSTOMERS_DIR'] = os.path.join(HERE, 'etc/ox/customers')

DB_CONNECTION_NAME = 'SNOWFLAKE'
PLATFORM_IDS = ["'test_platform_id_01'", "'test_platform_id_02'"]
QUERY = """
SELECT content_topic_nk
FROM content_topic_dim
WHERE platform_id in (%s)
ORDER BY content_topic_nk""" % ",".join(PLATFORM_IDS)
DELETE = """
DELETE FROM content_topic_dim
WHERE platform_id in (%s)""" % ",".join(PLATFORM_IDS)
RESULTS = [999999997, 999999998, 999999999]


class TestClient(unittest.TestCase):

    @ignore_warnings
    def setUp(self):
        Loader().run()

    def tearDown(self):
        with OXDB(DB_CONNECTION_NAME) as oxdb:
            oxdb.execute(DELETE)

    def test_results(self):
        with OXDB(DB_CONNECTION_NAME) as oxdb:
            for index, (content_topic_nk,) in enumerate(oxdb.get_executed_cursor(QUERY)):
                self.assertEqual(content_topic_nk, RESULTS[index])


if __name__ == '__main__':
    unittest.main()
