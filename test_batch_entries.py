import unittest
from unittest.mock import patch
#
import stg_to_delta
import main


class TestBatchEntries(unittest.TestCase):

    # @patch('stg_to_delta.create_batch_entry')
    @patch('main.get_create_batch_entry')
    def test_create_batch_entries(self, mock_request):

        curr_batch_id = 100
        batch_start_ts = '2023-02-10 08:15:44.287'
        delta_start_ts = '2023-02-10'
        delta_end_ts = '2023-02-10'

        mock_request.return_value = curr_batch_id, batch_start_ts, delta_start_ts, delta_end_ts

        # batch_id, batch_start, delta_start, delta_end = stg_to_delta.get_create_batch_entry()
        batch_id, batch_start, delta_start, delta_end = main.get_create_batch_entry()

        demo_cur_batch_id = 100
        demo_batch_start = '2023-02-10 08:15:44.287'
        demo_delta_start = '2023-02-10'
        demo_delta_end = '2023-02-10'

        json_return = {"cur_batch_id": batch_id, "batch_start_ts": batch_start, "delta_start_ts": delta_start, "delta_end_ts": delta_end}
        json_demo = {"cur_batch_id": demo_cur_batch_id, "batch_start_ts": demo_batch_start, "delta_start_ts": demo_delta_start, "delta_end_ts": demo_delta_end}

        self.assertEqual(batch_id, demo_cur_batch_id) and self.assertEqual(batch_start, demo_batch_start) and self.assertEqual(delta_start, demo_delta_start) and self.assertEqual(delta_end, demo_delta_end)

    # @patch('main.get_upsert_task_status')
    @patch('stg_to_delta.upsert_task_status')
    def test_upsert_status(self, mock_request):

        etl_control_task_id = 12
        etl_task_status = 'SUCCESS'

        mock_request.return_value = etl_control_task_id, etl_task_status

        # task_id, task_status = main.get_upsert_task_status()
        task_id, task_status = stg_to_delta.upsert_task_status()

        print(task_id)
        print(task_status)
        demo_task_id = 12
        demo_task_status = 'SUCCESS'

        json_return = {"task_id": task_id, "task_status": task_status}
        json_demo = {"task_id": demo_task_id, "task_status": demo_task_status}

        self.assertEqual(json_return, json_demo)

    # @patch('main.get_close_batch_status')
    @patch('stg_to_delta.close_batch_status')
    def test_close_status(self, mock_request):
        batch_id = 10
        previous_status = 'STARTED'
        batch_status = 'SUCCESS'

        mock_request.return_value = batch_id, previous_status, batch_status

        return_batch_id, return_previous_status, return_batch_status = main.get_close_batch_status()

        demo_batch_id = 10
        demo_previous_status = 'STARTED'
        demo_batch_status = 'SUCCESS'

        json_return = {"batch_id" : return_batch_id, "previous_state": return_previous_status, "batch_status": return_batch_status}
        json_demo = {"batch_id" : demo_batch_id, "previous_state": demo_previous_status, "batch_status": demo_batch_status}

        self.assertEqual(json_return, json_demo)

