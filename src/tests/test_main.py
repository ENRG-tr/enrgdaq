import unittest
from queue import Queue
from unittest.mock import MagicMock, patch

from daq.base import DAQJobThread
from daq.models import DAQJobMessage
from daq.store.base import DAQJobStore
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

    @patch("main.start_daq_job")
    @patch("main.restart_daq_job")
    def test_loop(self, mock_start_daq_job, mock_restart_daq_job):
        RUN_COUNT = 3
        mock_thread_alive = MagicMock(name="thread_alive")

        for _ in range(RUN_COUNT):
            mock_thread_dead = MagicMock(name="thread_dead")
            mock_thread_dead.daq_job = MagicMock()
            mock_thread_dead.daq_job.message_out = Queue()
            mock_thread_alive.daq_job.message_out = Queue()
            mock_thread_alive.thread.is_alive.return_value = True
            mock_thread_dead.thread.is_alive.return_value = False

            mock_thread_store = MagicMock(spec=DAQJobStore, name="thread_store")
            mock_thread_store.daq_job = MagicMock()
            mock_thread_store.daq_job.allowed_message_in_types = [MagicMock]
            mock_thread_store.daq_job.message_in = Queue()
            mock_thread_store.daq_job.message_out = Queue()
            mock_thread_store.daq_job.can_store = MagicMock(return_value=True)
            mock_thread_store.thread = MagicMock()
            mock_thread_store.thread.is_alive.return_value = True

            mock_store_message = MagicMock(spec=DAQJobMessageStore)
            mock_store_message.store_config = MagicMock()

            # we will expect this to be received by mock_thread_store
            mock_thread_alive.daq_job.message_out.put(mock_store_message)

            mock_start_daq_job.return_value = mock_thread_dead
            mock_restart_daq_job.return_value = mock_thread_store

            daq_job_threads = [mock_thread_alive, mock_thread_dead, mock_thread_store]
            daq_job_threads: list[DAQJobThread] = daq_job_threads

            # TODO: test stats
            result, _ = loop(daq_job_threads, {})

            self.assertEqual(
                result, [mock_thread_alive, mock_thread_store, mock_thread_dead]
            )
            self.assertEqual(mock_thread_alive.daq_job.message_out.qsize(), 0)
            self.assertEqual(mock_thread_store.daq_job.message_in.qsize(), 1)
        self.assertEqual(mock_start_daq_job.call_count, RUN_COUNT)

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
            store_config={}, keys=[], data=[], daq_job=MagicMock()
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
