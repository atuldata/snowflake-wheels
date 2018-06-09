from datetime import datetime
import unittest
from ox_dw_snowflake_currency_rate_loader import BASE_CURRENCY, Client


class TestClient(unittest.TestCase):

    def setUp(self):
        self.client = Client(BASE_CURRENCY, datetime.now())

    def test_fields(self):
        self.assertTrue(isinstance(self.client.fields, list))

    def test_one_to_one(self):
        self.assertEqual(
            self.client.one_to_one.get('base_currency'), BASE_CURRENCY)

    def test_iter(self):
        for row in self.client:
            self.assertTrue(isinstance(row, list))
            self.assertEqual(len(row), len(self.client.fields))


if __name__ == '__main__':
    unittest.main()
