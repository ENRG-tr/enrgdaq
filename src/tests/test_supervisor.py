import unittest
from datetime import datetime, timedelta
from queue import Queue
from unittest.mock import MagicMock, patch

from enrgdaq.daq.base import DAQJobInfo
from enrgdaq.daq.jobs.handle_stats import DAQJobMessageStats
from enrgdaq.daq.models import DAQJobMessage, DAQJobStats
from enrgdaq.daq.store.base import DAQJobStore
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
        self.supervisor.daq_job_processes = []
        self.supervisor.config = MagicMock()
        self.supervisor._logger = MagicMock()

    @patch("enrgdaq.supervisor.start_daq_jobs")
    @patch("enrgdaq.supervisor.load_daq_jobs")
    def test_start_daq_job_processes(self, mock_load_daq_jobs, mock_start_daq_jobs):
        mock_load_daq_jobs.return_value = ["job1", "job2"]
        mock_start_daq_jobs.return_value = ["thread1", "thread2"]

        result = self.supervisor.start_daq_job_processes()

        self.supervisor.daq_job_stats = {}
        self.supervisor.daq_job_processes = []

        mock_load_daq_jobs.assert_called_once_with("configs/", self.supervisor.config)
        mock_start_daq_jobs.assert_called_once_with(["job1", "job2"])
        self.assertEqual(result, ["thread1", "thread2"])

    @patch.object(Supervisor, "start_daq_job_processes")
    @patch.object(Supervisor, "warn_for_lack_of_daq_jobs")
    def test_init(self, mock_warn_for_lack_of_daq_jobs, mock_start_daq_job_processes):
        mock_process = MagicMock()
        mock_process.daq_job_cls = MagicMock()
        mock_start_daq_job_processes.return_value = [mock_process]

        self.supervisor.init()

        mock_start_daq_job_processes.assert_called_once()
        mock_warn_for_lack_of_daq_jobs.assert_called_once()
        self.assertEqual(self.supervisor.daq_job_processes, [mock_process])
        self.assertIn(mock_process.daq_job_cls, self.supervisor.daq_job_stats)
        self.assertIsInstance(
            self.supervisor.daq_job_stats[mock_process.daq_job_cls], DAQJobStats
        )

    @patch("enrgdaq.supervisor.start_daq_job")
    @patch.object(Supervisor, "get_messages_from_daq_jobs")
    @patch.object(Supervisor, "get_supervisor_messages")
    @patch.object(Supervisor, "send_messages_to_daq_jobs")
    def test_loop(
        self,
        mock_send_messages_to_daq_jobs,
        mock_get_supervisor_messages,
        mock_get_messages_from_daq_jobs,
        mock_start_daq_job,
    ):
        # TODO: Add this back
        return
        mock_daq_job_cls = MagicMock()
        mock_daq_job_cls.__name__ = "mock_job"

        mock_process_alive = MagicMock()
        mock_process_alive.daq_job_cls = mock_daq_job_cls
        mock_process_alive.process.is_alive.return_value = True
        mock_process_alive.start_time = datetime.now() - timedelta(seconds=5)
        mock_process_dead = MagicMock()
        mock_process_dead.daq_job_cls = mock_daq_job_cls
        mock_process_dead.process.is_alive.return_value = False
        mock_process_dead.start_time = datetime.now() - timedelta(seconds=10)
        mock_start_daq_job.return_value = mock_process_alive

        self.supervisor.daq_job_processes = [mock_process_alive, mock_process_dead]
        self.supervisor.daq_job_stats = {}
        self.supervisor.restart_schedules = []

        mock_get_messages_from_daq_jobs.return_value = ["message1"]
        mock_get_supervisor_messages.return_value = ["message2"]

        self.supervisor.loop()

        # mock_start_daq_job.assert_called_once_with(mock_process_dead)
        mock_get_messages_from_daq_jobs.assert_called_once()
        mock_get_supervisor_messages.assert_called_once()
        mock_send_messages_to_daq_jobs.assert_called_once()

        self.assertEqual(
            self.supervisor.daq_job_processes,
            [mock_process_alive, mock_start_daq_job.return_value],
        )

    def test_handle_thread_alive_stats(self):
        mock_alive_thread = MagicMock(name="alive_thread")
        mock_alive_thread.start_time = datetime.now() - timedelta(seconds=10)
        mock_dead_thread = MagicMock(name="dead_thread")
        mock_dead_thread.start_time = datetime.now() - timedelta(seconds=10)

        self.supervisor.daq_job_processes = [mock_alive_thread]
        self.supervisor.daq_job_stats = {
            type(mock_alive_thread.daq_job): DAQJobStats(),
            type(mock_dead_thread.daq_job): DAQJobStats(),
        }

        self.supervisor.handle_process_alive_stats([mock_dead_thread])

        # Test if dead_thread.is_alive = False and alive_thread.is_alive = True
        self.assertTrue(
            self.supervisor.daq_job_stats[mock_alive_thread.daq_job_cls].is_alive
        )
        self.assertFalse(
            self.supervisor.daq_job_stats[mock_dead_thread.daq_job_cls].is_alive
        )

        # Now restart the dead thread and run it for const + 1 seconds
        self.supervisor.daq_job_processes += [mock_dead_thread]

        mock_dead_thread.start_time = datetime.now() - timedelta(
            seconds=DAQ_JOB_MARK_AS_ALIVE_TIME_SECONDS + 1
        )
        self.supervisor.handle_process_alive_stats([])

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
        mock_daq_job_cls = MagicMock()
        mock_daq_job_cls.restart_offset = timedelta(seconds=10)
        mock_daq_job_cls.config = MagicMock()
        mock_daq_job_cls.__name__ = "mock_job"

        mock_process = MagicMock()
        mock_process.daq_job_cls = mock_daq_job_cls

        result = self.supervisor.get_restart_schedules([mock_process])

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].daq_job_process, mock_process)
        self.assertEqual(result[0].restart_at, datetime(2023, 1, 1, 12, 0, 10))

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
        mock_process = MagicMock()
        mock_process.message_out = Queue()
        mock_message = DAQJobMessage()
        mock_process.message_out.put(mock_message)

        self.supervisor.daq_job_processes = [mock_process]

        result = self.supervisor.get_messages_from_daq_jobs()

        self.assertEqual(result, [mock_message])

    def test_send_messages_to_daq_jobs(self):
        mock_process = MagicMock()
        mock_daq_job_cls = MagicMock(spec=DAQJobStore)
        mock_daq_job_cls.allowed_message_in_types = [DAQJobMessageStore]
        mock_process.daq_job_cls = mock_daq_job_cls
        mock_process.message_in = Queue()
        mock_message = DAQJobMessageStoreTabular(
            store_config=MagicMock(), keys=[], data=[], daq_job_info=DAQJobInfo.mock()
        )
        self.supervisor.daq_job_processes = [mock_process]

        self.supervisor.send_messages_to_daq_jobs([mock_message])

        self.assertFalse(mock_process.message_in.empty())
        self.assertEqual(
            mock_process.message_in.get(timeout=DAQ_JOB_QUEUE_ACTION_TIMEOUT),
            mock_message,
        )


if __name__ == "__main__":
    unittest.main()
