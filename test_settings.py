import json
import unittest
from unittest.mock import patch, MagicMock
import main


class TestClassSettings(unittest.TestCase):

    @patch('main.get_settings')
    def test_get_settings(self, mock_request):
        json1 = '{}'
        json1_dict = json.loads(json1)
        mock_request.return_value = json1_dict

        json2_dict = main.get_settings()

        json3 = '{}'
        # json3 = '{"a": "1"}'
        json3_dict = json.loads(json3)

        print(type(mock_request.return_value))
        self.assertEqual(json2_dict, json3_dict)

    @patch('main.get_staging_container')
    def test_get_staging_container(self, mock_request):

        json1 = '{}'
        json1_dict = json.loads(json1)
        mock_request.return_value = json1_dict

        json2_dict = main.get_staging_container()

        json3 = '{}'
        json3_dict = json.loads(json3)

        self.assertEqual(json2_dict, json3_dict)

    @patch('main.get_log_container')
    def test_get_log_container(self, mock_request):

        json1 = '{"a": "1"}'
        json1_dict = json.loads(json1)
        mock_request.return_value = json1_dict

        json2_dict = main.get_log_container()

        json3 = '{"a": "1"}'
        json3_dict = json.loads(json3)

        self.assertEqual(sorted(json2_dict.items()), sorted(json3_dict.items()))

    @patch('main.get_target_container')
    def test_get_target_container(self, mock_request):
        # json1 = {}
        json1 = '{"a":"first"}'
        json1_dict = json.loads(json1)
        mock_request.return_value = json1_dict

        json2_dict = main.get_target_container()

        json3 = '{"a":"first"}'
        json3_dict = json.loads(json3)

        self.assertEqual(sorted(json2_dict.items()), sorted(json3_dict.items()))
