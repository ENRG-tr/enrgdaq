import unittest
from queue import Queue
from unittest.mock import MagicMock, patch

from daq.base import DAQJobInfo, DAQJobThread
from daq.models import DAQJobMessage
from daq.store.models import DAQJobMessageStore
from main import (
    DAQ_JOB_QUEUE_ACTION_TIMEOUT,
    get_messages_from_daq_jobs,
    loop,
    send_messages_to_daq_jobs,
    start_daq_job_threads,
)


class TestMain(unittest.TestCase):
    @patch("main.start_daq_jobs")
    @patch("main.load_daq_jobs")
    def test_start_daq_job_threads(self, mock_load_daq_jobs, mock_start_daq_jobs):
        mock_load_daq_jobs.return_value = ["job1", "job2"]
        mock_start_daq_jobs.return_value = ["thread1", "thread2"]

        result = start_daq_job_threads()

        mock_load_daq_jobs.assert_called_once_with("configs/")
        mock_start_daq_jobs.assert_called_once_with(["job1", "job2"])
        self.assertEqual(result, ["thread1", "thread2"])

    @patch("main.restart_daq_job")
    @patch("main.get_messages_from_daq_jobs")
    @patch("main.get_supervisor_messages")
    @patch("main.send_messages_to_daq_jobs")
    def test_loop(
        self,
        mock_send_messages_to_daq_jobs,
        mock_get_supervisor_messages,
        mock_get_messages_from_daq_jobs,
        mock_restart_daq_job,
    ):
        mock_thread_alive = MagicMock()
        mock_thread_alive.thread.is_alive.return_value = True
        mock_thread_dead = MagicMock()
        mock_thread_dead.thread.is_alive.return_value = False
        mock_restart_daq_job.return_value = mock_thread_alive

        daq_job_threads: list[DAQJobThread] = [mock_thread_alive, mock_thread_dead]
        daq_job_stats = {}

        mock_get_messages_from_daq_jobs.return_value = ["message1"]
        mock_get_supervisor_messages.return_value = ["message2"]

        result_threads, result_stats = loop(daq_job_threads, daq_job_stats)

        mock_restart_daq_job.assert_called_once_with(mock_thread_dead.daq_job)
        mock_get_messages_from_daq_jobs.assert_called_once_with(
            [mock_thread_alive, mock_restart_daq_job.return_value], daq_job_stats
        )
        mock_get_supervisor_messages.assert_called_once_with(
            [mock_thread_alive, mock_restart_daq_job.return_value], daq_job_stats
        )
        mock_send_messages_to_daq_jobs.assert_called_once_with(
            [mock_thread_alive, mock_restart_daq_job.return_value],
            ["message1", "message2"],
            daq_job_stats,
        )

        self.assertEqual(
            result_threads, [mock_thread_alive, mock_restart_daq_job.return_value]
        )
        self.assertEqual(result_stats, daq_job_stats)

    def test_get_messages_from_daq_jobs(self):
        mock_thread = MagicMock()
        mock_thread.daq_job.message_out = Queue()
        mock_message = DAQJobMessage()
        mock_thread.daq_job.message_out.put(mock_message)

        daq_job_threads = [mock_thread]
        daq_job_threads: list[DAQJobThread] = daq_job_threads

        result = get_messages_from_daq_jobs(daq_job_threads, {})

        self.assertEqual(result, [mock_message])

    @patch("main.parse_store_config")
    def test_send_messages_to_daq_jobs(self, mock_parse_store_config):
        mock_thread = MagicMock()
        mock_thread.daq_job.allowed_message_in_types = [DAQJobMessageStore]
        mock_thread.daq_job.message_in = Queue()
        mock_message = DAQJobMessageStore(
            store_config={}, keys=[], data=[], daq_job_info=DAQJobInfo.mock()
        )
        daq_job_threads = [mock_thread]
        daq_job_threads: list[DAQJobThread] = daq_job_threads

        send_messages_to_daq_jobs(daq_job_threads, [mock_message], {})

        mock_parse_store_config.assert_called_once_with({})
        self.assertFalse(mock_thread.daq_job.message_in.empty())
        self.assertEqual(
            mock_thread.daq_job.message_in.get(timeout=DAQ_JOB_QUEUE_ACTION_TIMEOUT),
            mock_message,
        )


if __name__ == "__main__":
    unittest.main()
