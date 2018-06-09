import unittest
from ox_dw_snowflake_api_to_dw.converters.line_item_targeting import get_converted_rows


COLUMNS = '''
    line_item_nk
    modified_date
    updated_timestamp
    revision
    targeting_uid
    oxtl
    custom_targeting
    geo_targeting
    techno_targeting
    viewability_targeting
'''.split()


class TestConverterLineItemTargeting(unittest.TestCase):

    def test_equals(self):
        row = [None for _ in range(5)] + \
              ['(pub.custom.cat == "oliver")'] + \
              [None for _ in range(4)]
        row_dict = dict(zip(COLUMNS, next(get_converted_rows([row]))))
        self.assertEqual(
            row_dict['custom_targeting'], 'cat = ("oliver")')

    def test_equals_with_non_matching(self):
        row = [None for _ in range(5)] + \
              ['((ox.inventory.ad_unit_id intersects "389539") AND (pub.custom.contentid == "170785676"))'] + \
              [None for _ in range(4)]
        row_dict = dict(zip(COLUMNS, next(get_converted_rows([row]))))
        self.assertEqual(
            row_dict['custom_targeting'], 'contentid = ("170785676")')

    def test_equals_multi_same_key(self):
        row = [None for _ in range(5)] + \
              ['(ox.techno.device.manufacturer == "HTC" AND ox.techno.device.type == "Droid Eris")'] + \
              [None for _ in range(4)]
        row_dict = dict(zip(COLUMNS, next(get_converted_rows([row]))))
        self.assertIn(
            row_dict['techno_targeting'],
            ['device.manufacturer = ("HTC") device.type = ("Droid Eris")',
            'device.type = ("Droid Eris") device.manufacturer = ("HTC")'])

    def test_equals_multi_distinct(self):
        row = [None for _ in range(5)] + \
              ['((ox.geo.city == "barcelona" AND ox.geo.state == "barcelona" AND ox.geo.country == "es"))'] + \
              [None for _ in range(4)]
        row_dict = dict(zip(COLUMNS, next(get_converted_rows([row]))))
        self.assertIn(
            row_dict['geo_targeting'],
            ['city = ("barcelona") state = ("barcelona") country = ("es")',
             'city = ("barcelona") country = ("es") state = ("barcelona")',
             'state = ("barcelona") city = ("barcelona") country = ("es")',
             'state = ("barcelona") country = ("es") city = ("barcelona")',
             'country = ("es") city = ("barcelona") state = ("barcelona")',
             'country = ("es") state = ("barcelona") city = ("barcelona")'])

    def test_intersects(self):
        row = [None for _ in range(5)] + \
              ['(ox.geo.country intersects "pl")'] + \
              [None for _ in range(4)]
        row_dict = dict(zip(COLUMNS, next(get_converted_rows([row]))))
        self.assertEqual(
            row_dict['geo_targeting'],
            'country = ("pl")')

    def test_intersects_multi_distinct(self):
        row = [None for _ in range(5)] + \
              ['(((pub.custom.id_01 intersects "361834") AND (pub.custom.id_02 intersects "4001,5001,6001,2001,3001,1")) AND (ox.geo.dma intersects "617"))'] + \
              [None for _ in range(4)]
        row_dict = dict(zip(COLUMNS, next(get_converted_rows([row]))))
        # Can come back out of order.
        self.assertIn(
            row_dict['custom_targeting'],
            ['id_01 = ("361834") id_02 = ("4001,5001,6001,2001,3001,1")',
            'id_02 = ("4001,5001,6001,2001,3001,1") id_01 = ("361834")'])
        self.assertEqual(
            row_dict['geo_targeting'],
            'dma = ("617")')


if __name__ == '__main__':
    unittest.main()
