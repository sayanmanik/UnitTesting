import unittest
from unittest import mock
from unittest.mock import patch, MagicMock

import main
from main import env_param, app_acronym_param, source_sys_param


@patch('main.ENV', 'nprod_dev')
@patch('main.APP_ACRONYM', 'oma')
@patch('main.SOURCE_SYS', 'amac')
class TestClassEnv(unittest.TestCase):

    def test_env(self):
        # mock_output.return_value = ''
        self.assertIn(main.ENV, main.env)

    def test_acronym(self):
        self.assertEqual(main.APP_ACRONYM, '')

    def test_source_sys(self):
        self.assertEqual(main.SOURCE_SYS, 'amac')
