from ox_dw_load_state import parse_date_string
from time import sleep

VARIABLE_NAME = 'TEST_LOAD_STATE'
VARIABLE_VALUE = 'This is a test value.'
VARIBALE_NEW_VALUE = 'This is a new test value.'
VARIABLE_VALUE_DATES = [{
    'current_value': '2016-09-01_10 UTC',
    'older_value': '2016-08-01_10 UTC',
    'newer_value': '2016-10-01_10 UTC'
}, {
    'current_value': '2016-09-01 10:11:12',
    'older_value': '2016-08-01 10:11:12',
    'newer_value': '2016-10-01 10:11:12'
}]
INTRVL_VALUE = '2016-10-01_23'


class LoadStateBase(object):

    def test_delete(self):
        self.load_state.delete()
        self.assertFalse(self.load_state.exists,
                         'Does not exist after delete.')

    def test_upsert(self):
        self.load_state.upsert(VARIABLE_VALUE, commit=True)
        modified_datetime = self.load_state.modified_datetime
        sleep(1)
        self.assertEqual(self.load_state.variable_value, VARIABLE_VALUE,
                         'Initial upsert.')
        self.load_state.upsert(VARIBALE_NEW_VALUE, commit=True)
        self.assertEqual(self.load_state.variable_value, VARIBALE_NEW_VALUE,
                         'Second upsert.')
        self.assertTrue(modified_datetime < self.load_state.modified_datetime,
                        'Modified time is greater after second upsert.')

    def test_variable_name(self):
        self.assertEqual(self.load_state.variable_name, VARIABLE_NAME)

    def test_variable_value(self):
        self.load_state.upsert(VARIABLE_VALUE, commit=True)
        self.assertEqual(self.load_state.variable_value, VARIABLE_VALUE)

    def test_variable_date_time(self):
        for dates in VARIABLE_VALUE_DATES:
            self.load_state.upsert(
                parse_date_string(
                    dates['current_value']).strftime('%Y-%m-%d %H:%M:%S'),
                commit=True)
            current_value = parse_date_string(self.load_state.variable_value)
            self.load_state.update_variable_datetime(
                parse_date_string(
                    dates['older_value']).strftime('%Y-%m-%d %H:%M:%S'),
                commit=True)
            new_value = parse_date_string(self.load_state.variable_value)
            self.assertEqual(current_value, new_value,
                             'Should not have changed')
            self.load_state.update_variable_datetime(
                parse_date_string(
                    dates['newer_value']).strftime('%Y-%m-%d %H:%M:%S'),
                commit=True)
            new_value = parse_date_string(self.load_state.variable_value)
            self.assertNotEqual(current_value, new_value,
                                'Should have changed')

    def test_update_variable_intrvl(self):
        self.load_state.update_variable_intrvl(INTRVL_VALUE)
        self.assertEqual(self.load_state.variable_value,
                         parse_date_string(INTRVL_VALUE + ' UTC').strftime(
                             '%Y-%m-%d %H:%M:%S'))
