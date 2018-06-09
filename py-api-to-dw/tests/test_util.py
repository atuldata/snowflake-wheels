import os
import sys
import unittest

from ox_dw_snowflake_api_to_dw.util import generate_rows, is_empty_file

HERE = \
    os.path.join(
        os.path.abspath(
            os.path.join(os.path.dirname(__file__))))
EMPTY_FILE = os.path.join(HERE, 'zero_byte_file.empty')
TEST_COLS = [
    'openx_rev_share_tier.tier_maximum',
    'openx_rev_share_tier.rev_share',
    'deleted',
    'revision',
    'how.deep.can.we.go'
]
TEST_OBJ_01 = {
    'openx_rev_share_tier': [
        {'tier_maximum': None, 'rev_share': '13.00'}
    ],
    'deleted': '0',
    'revision': 1,
    'how': {'deep': {'can': {'we': {'go': '?'}}}}
}
TEST_RESULTS_01 = [
    [None, '13.00', '0', 1, '?']
]
TEST_OBJ_02 = {
    'openx_rev_share_tier': [
        {'tier_maximum': None, 'rev_share': '13.00'},
        {'tier_maximum': 1000, 'rev_share': '15.00'}
    ],
    'deleted': '1',
    'revision': 2,
    'how': {'deep': {'can': {'we': {'go': '?'}}}}
}
TEST_RESULTS_02 = [
    [None, '13.00', '1', 2, '?'],
    [1000, '15.00', '1', 2, '?']
]
TEST_COLS_03 = [
    'openx_rev_share_tier.tier_maximum',
    'openx_rev_share_tier.rev_share',
    'deleted',
    'revision',
    'transaction_from_demand_partner.rev_share',
    'transaction_from_demand_partner.demand_partner_id',
    'how.deep.can.we.go'
]
TEST_OBJ_03 = {
    'openx_rev_share_tier': [
        {'tier_maximum': None, 'rev_share': '13.00'},
        {'tier_maximum': 1000, 'rev_share': '15.00'}
    ],
    'deleted': '1',
    'revision': 2,
    'transaction_from_demand_partner': [
        {'rev_share': '15.00', 'demand_partner_id': '1610613736'},
        {'rev_share': '16.00', 'demand_partner_id': '1610613752'}
    ],
    'how': {'deep': {'can': {'we': {'go': '?'}}}}
}
TEST_RESULTS_03 = [
    [None, '13.00', '1', 2, '15.00', '1610613736', '?'],
    [None, '13.00', '1', 2, '16.00', '1610613752', '?'],
    [1000, '15.00', '1', 2, '15.00', '1610613736', '?'],
    [1000, '15.00', '1', 2, '16.00', '1610613752', '?']
]
TEST_COLS_04 = [
    'openx_rev_share_tier.tier_maximum',
    'openx_rev_share_tier.rev_share',
    'deleted',
    'revision',
    'transaction_from_demand_partner.rev_share',
    'transaction_from_demand_partner.demand_partner_id',
    'how.deep.can.we.go',
    'list_multiplier'
]
TEST_OBJ_04 = {
    'openx_rev_share_tier': [
        {'tier_maximum': None, 'rev_share': '13.00'},
        {'tier_maximum': 1000, 'rev_share': '15.00'}
    ],
    'deleted': '1',
    'revision': 2,
    'transaction_from_demand_partner': [
        {'rev_share': '15.00', 'demand_partner_id': '1610613736'},
        {'rev_share': '16.00', 'demand_partner_id': '1610613752'}
    ],
    'how': {'deep': {'can': {'we': {'go': '?'}}}},
    'list_multiplier': [1, 2, 3]
}
TEST_RESULTS_04 = [
    [None, '13.00', '1', 2, '15.00', '1610613736', '?', 1],
    [None, '13.00', '1', 2, '15.00', '1610613736', '?', 2],
    [None, '13.00', '1', 2, '15.00', '1610613736', '?', 3],
    [None, '13.00', '1', 2, '16.00', '1610613752', '?', 1],
    [None, '13.00', '1', 2, '16.00', '1610613752', '?', 2],
    [None, '13.00', '1', 2, '16.00', '1610613752', '?', 3],
    [1000, '15.00', '1', 2, '15.00', '1610613736', '?', 1],
    [1000, '15.00', '1', 2, '15.00', '1610613736', '?', 2],
    [1000, '15.00', '1', 2, '15.00', '1610613736', '?', 3],
    [1000, '15.00', '1', 2, '16.00', '1610613752', '?', 1],
    [1000, '15.00', '1', 2, '16.00', '1610613752', '?', 2],
    [1000, '15.00', '1', 2, '16.00', '1610613752', '?', 3]
]
TEST_COLS_05 = [
    'how.deep.can.we.go',
    'deep.down.list.multiplier',
    'deleted',
    'revision',
    'deep.down.list.multiplier'
]
TEST_OBJ_05 = {
    'deleted': '1',
    'revision': 2,
    'how': {'deep': {'can': {'we': {'go': '?'}}}},
    'deep': {'down': {'list': {'multiplier': [1, 2, 3]}}}
}
TEST_RESULTS_05 = [
    ['?', 1, '1', 2, 1],
    ['?', 2, '1', 2, 2],
    ['?', 3, '1', 2, 3]
]
TEST_COLS_06 = [
    'how.deep.can.we.go',
    'deep.down.list.multiplier2',
    'deleted',
    'revision',
    'deep.down.list.multiplier'
]
TEST_OBJ_06 = {
    'deleted': '1',
    'revision': 2,
    'how': {'deep': {'can': {'we': {'go': '?'}}}},
    'deep': {'down': {'list': {
        'multiplier': [1, 2, 3],
        'multiplier2': [4, 5, 6]}}}
}
TEST_RESULTS_06 = [
    ['?', 4, '1', 2, 1],
    ['?', 4, '1', 2, 2],
    ['?', 4, '1', 2, 3],
    ['?', 5, '1', 2, 1],
    ['?', 5, '1', 2, 2],
    ['?', 5, '1', 2, 3],
    ['?', 6, '1', 2, 1],
    ['?', 6, '1', 2, 2],
    ['?', 6, '1', 2, 3]
]
TEST_COLS_07 = set([
    'revision',
    'deep.dict.list.rev_share',
    'deep.dict.list.dps'
])
TEST_OBJ_07 = {
    'revision': 100,
    'deep': {'dict': {'list': [{'rev_share': 10, 'dps': 10}]}}
}
TEST_RESULTS_07 = [

]
TEST_COLS_08 = [
    'revision',
    'hello',
    'exchange.domains.KEYS',
    'exchange.domains.VALUES'
]
TEST_OBJ_08 = {
    'revision': 1,
    'hello': 'Good Bye',
    'exchange': {
        'domains': {
            'test.com': True,
            '*.example.com': True}
    }
}
TEST_RESULTS_08 = [
    [1, 'Good Bye', '*.example.com', True],
    [1, 'Good Bye', 'test.com', True]
]


class TestUtil(unittest.TestCase):

    def test_is_empty_file_false(self):
        self.assertFalse(is_empty_file(sys.argv[0]))

    def test_is_empty_file_true(self):
        self.assertTrue(is_empty_file(EMPTY_FILE))

    def test_row_01(self):
        """
        This set shows that a list of dicts will be extracted by dot notation.
        """
        for index, row in enumerate(generate_rows(TEST_OBJ_01, TEST_COLS)):
            self.assertEqual(row, TEST_RESULTS_01[index])

    def test_row_02(self):
        """
        This set shows that a list of dicts of more than one will results
        in more rows being returned. Also has example of drilling down dicts
        of dicts using dot notation.
        """
        for index, row in enumerate(generate_rows(TEST_OBJ_02, TEST_COLS)):
            self.assertEqual(row, TEST_RESULTS_02[index])

    def test_row_03(self):
        """
        This set has 2 lists of dicts and demonstrates the row multiplication.
        """
        for index, row in enumerate(generate_rows(TEST_OBJ_03, TEST_COLS_03)):
            self.assertEqual(row, TEST_RESULTS_03[index])

    def test_row_04(self):
        """
        This set demonstrates how a list can multiply the rows.
        """
        for index, row in enumerate(generate_rows(TEST_OBJ_04, TEST_COLS_04)):
            self.assertEqual(row, TEST_RESULTS_04[index])

    def test_row_05(self):
        """
        This set demonstrates how a list can multiply the rows from a deep
        dict list value. Also demonstrates that having the same list twice in
        the results will not double the row mulplication.
        """
        for index, row in enumerate(generate_rows(TEST_OBJ_05, TEST_COLS_05)):
            self.assertEqual(row, TEST_RESULTS_05[index])

    def test_row_06(self):
        """
        This set demonstrates how a list can multiply the rows from a deep
        dict list value. Also demonstrates that having a second list will
        cause even more multiplication of the rows.
        """
        for index, row in enumerate(generate_rows(TEST_OBJ_06, TEST_COLS_06)):
            self.assertEqual(row, TEST_RESULTS_06[index])

    def test_row_07(self):
        """
        This set demonstrates how a list can multiply the rows from a deep
        dict list value along side a int value of the same dict.
        TODO: No use case yet but should work. Will come back to it later.
        from ox_api_to_dw.util import get_walked_obj
        print('')
        print(TEST_OBJ_07)
        print(get_walked_obj(TEST_OBJ_07))
        for index, row in enumerate(generate_rows(TEST_OBJ_07, TEST_COLS_07)):
            print(row)
            #self.assertEqual(row, TEST_RESULTS_07[index])
        """
        pass

    def test_row_08(self):
        """
        This test demonstrates pulling the keys/values from a dict. This test
        distincts the rows as when you call by 2 separate lists the rows are
        duped.
        """
        self.assertEqual(
            sorted([list(x) for x in set(tuple(x)
                    for x in generate_rows(TEST_OBJ_08, TEST_COLS_08))]),
            sorted(TEST_RESULTS_08))


if __name__ == '__main__':
    unittest.main()
