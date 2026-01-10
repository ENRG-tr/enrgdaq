import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from enrgdaq.models import SupervisorCNCConfig, SupervisorConfig, SupervisorInfo
from enrgdaq.supervisor import Supervisor


class TestSupervisor(unittest.TestCase):
    def setUp(self):
        # Mock logging to avoid printing messages
        self.patcher = patch("enrgdaq.supervisor.logging")
        self.mock_logging = self.patcher.start()
        self.addCleanup(self.patcher.stop)

        self.supervisor = Supervisor()
        self.supervisor.daq_job_stats = {}
        self.supervisor.daq_job_processes = []
        self.supervisor.config = SupervisorConfig(
            info=SupervisorInfo(supervisor_id="test"), cnc=None
        )
        self.supervisor._logger = MagicMock()

    @patch("enrgdaq.supervisor.start_daq_jobs")
    @patch("enrgdaq.supervisor.load_daq_jobs")
    def test_start_daq_job_processes(self, mock_load_daq_jobs, mock_start_daq_jobs):
        mock_job1 = MagicMock()
        mock_job2 = MagicMock()
        mock_load_daq_jobs.return_value = [mock_job1, mock_job2]
        mock_start_daq_jobs.return_value = ["thread1", "thread2"]

        self.supervisor.start_daq_job_processes([])

        assert self.supervisor.config
        mock_load_daq_jobs.assert_called_once_with(
            "configs/", self.supervisor.config.info
        )
        mock_start_daq_jobs.assert_called_once_with([mock_job1, mock_job2])
        self.assertEqual(self.supervisor.daq_job_processes, ["thread1", "thread2"])

    @patch.object(Supervisor, "warn_for_lack_of_daq_jobs")
    def test_init(self, mock_warn_for_lack_of_daq_jobs):
        mock_process = MagicMock()
        mock_process.daq_job_cls = MagicMock()
        mock_process.daq_job_cls.__name__ = "mock_job"
        self.supervisor.config.cnc = SupervisorCNCConfig()
        self.supervisor._daq_jobs_to_load = [mock_process]

        self.supervisor.init()

        mock_warn_for_lack_of_daq_jobs.assert_called_once()
        self.assertEqual(self.supervisor.daq_job_processes, [mock_process])

    @patch("enrgdaq.supervisor.start_daq_job")
    def test_loop(self, mock_start_daq_job):
        # TODO: Add this back - loop test needs rework after stats refactoring
        pass

    # test_handle_thread_alive_stats removed - method moved to DAQJobs

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
    def test_get_restart_schedules_no_restart_on_crash(self, mock_datetime):
        """Test that processes with restart_on_crash=False are not scheduled for restart."""
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
        mock_daq_job_cls = MagicMock()
        mock_daq_job_cls.restart_offset = timedelta(seconds=10)
        mock_daq_job_cls.config = MagicMock()
        mock_daq_job_cls.__name__ = "mock_job"

        # Process with restart_on_crash=False should not be scheduled
        mock_process_no_restart = MagicMock()
        mock_process_no_restart.daq_job_cls = mock_daq_job_cls
        mock_process_no_restart.restart_on_crash = False

        # Process with restart_on_crash=True should be scheduled
        mock_process_with_restart = MagicMock()
        mock_process_with_restart.daq_job_cls = mock_daq_job_cls
        mock_process_with_restart.restart_on_crash = True

        result = self.supervisor.get_restart_schedules(
            [mock_process_no_restart, mock_process_with_restart]
        )

        # Only the process with restart_on_crash=True should be in the result
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].daq_job_process, mock_process_with_restart)
        self.assertEqual(result[0].restart_at, datetime(2023, 1, 1, 12, 0, 10))

    # test_get_supervisor_messages removed - method removed from Supervisor
    # test_get_messages_from_daq_jobs removed - method removed from Supervisor
    # test_send_messages_to_daq_jobs removed - method removed from Supervisor


if __name__ == "__main__":
    unittest.main()
