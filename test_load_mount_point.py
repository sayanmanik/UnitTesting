import unittest
import main
from unittest.mock import patch
import json


class TestLoadMountPoint(unittest.TestCase):

    @patch('main.get_staging_container')
    def test_mount_point(self, mock_request):

        json_container = '{"container": "ins-dl-amac-log"}'
        dict_container = json.loads(json_container)
        mock_request.return_value = dict_container

        param = 'StagingContainer'
        container = main.get_containers(param)
        print(type(container))
        print(container)

        # json_demo = '{"container": "ins-dl-amac-log"}'
        json_demo = '{}'

        dict_demo = json.loads(json_demo)

        self.assertEqual(sorted(container.items()), sorted(dict_demo.items()))