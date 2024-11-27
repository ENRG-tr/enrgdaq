import unittest
from datetime import datetime, timedelta
from queue import Queue
from unittest.mock import MagicMock, patch

from enrgdaq.daq.base import DAQJobInfo
from enrgdaq.daq.jobs.handle_stats import DAQJobMessageStats
from enrgdaq.daq.models import DAQJobMessage, DAQJobStats
from enrgdaq.daq.store.models import DAQJobMessageStore, DAQJobMessageStoreTabular
from enrgdaq.supervisor import (
    DAQ_JOB_MARK_AS_ALIVE_TIME_SECONDS,
    DAQ_JOB_QUEUE_ACTION_TIMEOUT,
    Supervisor,
)


class TestSupervisor(unittest.TestCase):
    def setUp(self):
        # Mock logging to avoid printing messages
        self.patcher = patch("enrgdaq.supervisor.logging")
        self.mock_logging = self.patcher.start()
        self.addCleanup(self.patcher.stop)

        self.supervisor = Supervisor()
        self.supervisor.daq_job_stats = {}
        self.supervisor.daq_job_threads = []
        self.supervisor.config = MagicMock()
        self.supervisor._logger = MagicMock()

    @patch("enrgdaq.supervisor.start_daq_jobs")
    @patch("enrgdaq.supervisor.load_daq_jobs")
    def test_start_daq_job_threads(self, mock_load_daq_jobs, mock_start_daq_jobs):
        mock_load_daq_jobs.return_value = ["job1", "job2"]
        mock_start_daq_jobs.return_value = ["thread1", "thread2"]

        result = self.supervisor.start_daq_job_threads()

        self.supervisor.daq_job_stats = {}
        self.supervisor.daq_job_threads = []

        mock_load_daq_jobs.assert_called_once_with("configs/", self.supervisor.config)
        mock_start_daq_jobs.assert_called_once_with(["job1", "job2"])
        self.assertEqual(result, ["thread1", "thread2"])

    @patch.object(Supervisor, "start_daq_job_threads")
    @patch.object(Supervisor, "warn_for_lack_of_daq_jobs")
    def test_init(self, mock_warn_for_lack_of_daq_jobs, mock_start_daq_job_threads):
        mock_thread = MagicMock()
        mock_thread.daq_job = MagicMock()
        mock_start_daq_job_threads.return_value = [mock_thread]

        self.supervisor.init()

        mock_start_daq_job_threads.assert_called_once()
        mock_warn_for_lack_of_daq_jobs.assert_called_once()
        self.assertEqual(self.supervisor.daq_job_threads, [mock_thread])
        self.assertIn(type(mock_thread.daq_job), self.supervisor.daq_job_stats)
        self.assertIsInstance(
            self.supervisor.daq_job_stats[type(mock_thread.daq_job)], DAQJobStats
        )

    @patch("enrgdaq.supervisor.restart_daq_job")
    @patch.object(Supervisor, "get_messages_from_daq_jobs")
    @patch.object(Supervisor, "get_supervisor_messages")
    @patch.object(Supervisor, "send_messages_to_daq_jobs")
    def test_loop(
        self,
        mock_send_messages_to_daq_jobs,
        mock_get_supervisor_messages,
        mock_get_messages_from_daq_jobs,
        mock_restart_daq_job,
    ):
        mock_thread_alive = MagicMock()
        mock_thread_alive.thread.is_alive.return_value = True
        mock_thread_alive.start_time = datetime.now() - timedelta(seconds=5)
        mock_thread_dead = MagicMock()
        mock_thread_dead.thread.is_alive.return_value = False
        mock_thread_dead.start_time = datetime.now() - timedelta(seconds=10)
        mock_restart_daq_job.return_value = mock_thread_alive

        self.supervisor.daq_job_threads = [mock_thread_alive, mock_thread_dead]
        self.supervisor.daq_job_stats = {}
        self.supervisor.restart_schedules = []

        mock_get_messages_from_daq_jobs.return_value = ["message1"]
        mock_get_supervisor_messages.return_value = ["message2"]

        self.supervisor.loop()

        mock_restart_daq_job.assert_called_once_with(
            type(mock_thread_dead.daq_job),
            mock_thread_dead.daq_job.config,
            self.supervisor.config,
        )
        mock_get_messages_from_daq_jobs.assert_called_once()
        mock_get_supervisor_messages.assert_called_once()
        mock_send_messages_to_daq_jobs.assert_called_once()

        self.assertEqual(
            self.supervisor.daq_job_threads,
            [mock_thread_alive, mock_restart_daq_job.return_value],
        )

    def test_handle_thread_alive_stats(self):
        mock_alive_thread = MagicMock(name="alive_thread")
        mock_alive_thread.start_time = datetime.now() - timedelta(seconds=10)
        mock_dead_thread = MagicMock(name="dead_thread")
        mock_dead_thread.start_time = datetime.now() - timedelta(seconds=10)

        self.supervisor.daq_job_threads = [mock_alive_thread]
        self.supervisor.daq_job_stats = {
            type(mock_alive_thread.daq_job): DAQJobStats(),
            type(mock_dead_thread.daq_job): DAQJobStats(),
        }

        self.supervisor.handle_thread_alive_stats([mock_dead_thread])

        # Test if dead_thread.is_alive = False and alive_thread.is_alive = True
        self.assertTrue(
            self.supervisor.daq_job_stats[type(mock_alive_thread.daq_job)].is_alive
        )
        self.assertFalse(
            self.supervisor.daq_job_stats[type(mock_dead_thread.daq_job)].is_alive
        )

        # Now restart the dead thread and run it for const + 1 seconds
        self.supervisor.daq_job_threads += [mock_dead_thread]

        mock_dead_thread.start_time = datetime.now() - timedelta(
            seconds=DAQ_JOB_MARK_AS_ALIVE_TIME_SECONDS + 1
        )
        self.supervisor.handle_thread_alive_stats([])

        # Test if dead_thread.is_alive = True and alive_thread.is_alive = True
        self.assertTrue(
            self.supervisor.daq_job_stats[type(mock_dead_thread.daq_job)].is_alive
        )
        self.assertTrue(
            self.supervisor.daq_job_stats[type(mock_alive_thread.daq_job)].is_alive
        )

    @patch("enrgdaq.supervisor.datetime")
    def test_get_restart_schedules(self, mock_datetime):
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
        mock_thread = MagicMock()
        mock_thread.daq_job.restart_offset = timedelta(seconds=10)
        mock_thread.daq_job.config = MagicMock()

        result = self.supervisor.get_restart_schedules([mock_thread])

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].daq_job_type, type(mock_thread.daq_job))
        self.assertEqual(result[0].daq_job_config, mock_thread.daq_job.config)
        self.assertEqual(result[0].restart_at, datetime(2023, 1, 1, 12, 0, 10))

    @patch("enrgdaq.supervisor.datetime")
    def test_restart_daq_jobs(self, mock_datetime):
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
        mock_schedule = MagicMock()
        mock_schedule.restart_at = datetime(2023, 1, 1, 12, 0, 0)
        mock_schedule.daq_job_type = MagicMock()
        mock_schedule.daq_job_config = MagicMock()

        self.supervisor.restart_schedules = [mock_schedule]
        self.supervisor.daq_job_stats = {}

        with patch("enrgdaq.supervisor.restart_daq_job") as mock_restart_daq_job:
            mock_restart_daq_job.return_value = "new_thread"
            self.supervisor.restart_daq_jobs()

            mock_restart_daq_job.assert_called_once_with(
                mock_schedule.daq_job_type,
                mock_schedule.daq_job_config,
                self.supervisor.config,
            )
            self.assertIn("new_thread", self.supervisor.daq_job_threads)
            self.assertEqual(len(self.supervisor.restart_schedules), 0)

    @patch("enrgdaq.supervisor.datetime")
    def test_get_supervisor_messages(self, mock_datetime):
        now = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = now
        self.supervisor._last_stats_message_time = now - timedelta(seconds=10)
        self.supervisor.daq_job_stats = {
            type(MagicMock()): DAQJobStats(),
            type(MagicMock()): DAQJobStats(),
        }
        self.supervisor.config = MagicMock()

        result = self.supervisor.get_supervisor_messages()

        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], DAQJobMessageStats)
        assert isinstance(result[0], DAQJobMessageStats)
        self.assertEqual(result[0].stats, self.supervisor.daq_job_stats)
        assert result[0].daq_job_info
        self.assertEqual(result[0].daq_job_info.daq_job_type, "Supervisor")

    def test_get_messages_from_daq_jobs(self):
        mock_thread = MagicMock()
        mock_thread.daq_job.message_out = Queue()
        mock_message = DAQJobMessage()
        mock_thread.daq_job.message_out.put(mock_message)

        self.supervisor.daq_job_threads = [mock_thread]

        result = self.supervisor.get_messages_from_daq_jobs()

        self.assertEqual(result, [mock_message])

    def test_send_messages_to_daq_jobs(self):
        mock_thread = MagicMock()
        mock_thread.daq_job.allowed_message_in_types = [DAQJobMessageStore]
        mock_thread.daq_job.message_in = Queue()
        mock_message = DAQJobMessageStoreTabular(
            store_config=MagicMock(), keys=[], data=[], daq_job_info=DAQJobInfo.mock()
        )
        self.supervisor.daq_job_threads = [mock_thread]

        self.supervisor.send_messages_to_daq_jobs([mock_message])

        self.assertFalse(mock_thread.daq_job.message_in.empty())
        self.assertEqual(
            mock_thread.daq_job.message_in.get(timeout=DAQ_JOB_QUEUE_ACTION_TIMEOUT),
            mock_message,
        )


if __name__ == "__main__":
    unittest.main()
