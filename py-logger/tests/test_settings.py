import unittest
from ox_dw_logger import get_config


class TestSettings(unittest.TestCase):

    def test_get_config_empty(self):
        config = get_config('asdasd')
        self.assertTrue(isinstance(config, dict))
        self.assertFalse(bool(config))

    def test_get_config(self):
        config = get_config('env.sample')
        self.assertTrue(isinstance(config, dict))
        self.assertTrue(bool(config))
        self.assertTrue(bool(config.get('VERTICA_DSN')))


if __name__ == '__main__':
    unittest.main()
