import unittest
import warnings
from simple_salesforce import Salesforce
from ox_dw_snowflake_salesforce_pull.salesforce_objects import SalesForceObjects

# Using a relatively small one for the testing
TEST_OBJECT_NAME = 'AXB_Account__c'


def ignore_warnings(test_func):
    """
    The test suite raises a ResourceWarning that does not appear in
    normal operation
    """
    def do_test(self, *args, **kwargs):
        warnings.simplefilter("ignore", ResourceWarning)
        with warnings.catch_warnings():
            test_func(self, *args, **kwargs)
    return do_test


class TestSalesForceObjects(unittest.TestCase):

    @ignore_warnings
    def setUp(self):
        self.sfo = SalesForceObjects([TEST_OBJECT_NAME], True)

    def test_client(self):
        self.assertIsInstance(self.sfo.client, Salesforce)

    def test_main(self):
        self.assertEqual(self.sfo.main(), None)


if __name__ == '__main__':
    unittest.main()
