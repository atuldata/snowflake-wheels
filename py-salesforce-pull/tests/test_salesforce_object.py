import unittest
import warnings
from ox_dw_snowflake_salesforce_pull.salesforce_object import SalesForceObject
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


class TestSalesForceObject(unittest.TestCase):

    @ignore_warnings
    def test_main(self):
        sfos = SalesForceObjects()
        self.assertEqual(
            SalesForceObject(TEST_OBJECT_NAME, sfos.client, sfos.oxdb).main(),
            None)


if __name__ == '__main__':
    unittest.main()
