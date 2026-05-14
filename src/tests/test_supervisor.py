import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from psutil import NoSuchProcess

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

    # --- _increment_restart_counter ---

    def test_increment_restart_counter_first_time(self):
        """First increment should create stats entry and set count to 1."""
        mock_process = MagicMock()
        mock_process.daq_job_cls.__name__ = "TestJob"

        self.supervisor._increment_restart_counter(mock_process)

        sup_id = self.supervisor.supervisor_id
        self.assertIn(sup_id, self.supervisor.daq_job_stats)
        self.assertIn("TestJob", self.supervisor.daq_job_stats[sup_id])
        self.assertEqual(
            self.supervisor.daq_job_stats[sup_id]["TestJob"].restart_stats.count, 1
        )

    def test_increment_restart_counter_multiple(self):
        """Multiple increments should accumulate."""
        mock_process = MagicMock()
        mock_process.daq_job_cls.__name__ = "TestJob"

        for _ in range(5):
            self.supervisor._increment_restart_counter(mock_process)

        sup_id = self.supervisor.supervisor_id
        self.assertEqual(
            self.supervisor.daq_job_stats[sup_id]["TestJob"].restart_stats.count, 5
        )

    def test_increment_restart_counter_multiple_job_types(self):
        """Different job types should have independent counters."""
        mock_job_a = MagicMock()
        mock_job_a.daq_job_cls.__name__ = "JobA"
        mock_job_b = MagicMock()
        mock_job_b.daq_job_cls.__name__ = "JobB"

        self.supervisor._increment_restart_counter(mock_job_a)
        self.supervisor._increment_restart_counter(mock_job_a)
        self.supervisor._increment_restart_counter(mock_job_b)

        sup_id = self.supervisor.supervisor_id
        self.assertEqual(
            self.supervisor.daq_job_stats[sup_id]["JobA"].restart_stats.count, 2
        )
        self.assertEqual(
            self.supervisor.daq_job_stats[sup_id]["JobB"].restart_stats.count, 1
        )

    # --- _collect_resource_stats ---

    @patch("enrgdaq.supervisor.psutil")
    def test_collect_resource_stats_populates_rss_and_cpu(self, mock_psutil):
        """Verify that RSS and CPU are polled and stored per job."""
        mock_proc1 = MagicMock()
        mock_proc1.process.pid = 12345
        mock_proc1.process.is_alive.return_value = True
        mock_proc1.daq_job_cls.__name__ = "DigitizerJob"

        mock_proc2 = MagicMock()
        mock_proc2.process.pid = 12346
        mock_proc2.process.is_alive.return_value = True
        mock_proc2.daq_job_cls.__name__ = "StoreJob"

        self.supervisor.daq_job_processes = [mock_proc1, mock_proc2]

        # Mock psutil.Process instances
        mock_psutil_proc1 = MagicMock()
        mock_psutil_proc1.memory_info.return_value.rss = 150 * 1024 * 1024  # 150 MB
        mock_psutil_proc1.cpu_percent.return_value = 12.5

        mock_psutil_proc2 = MagicMock()
        mock_psutil_proc2.memory_info.return_value.rss = 80 * 1024 * 1024  # 80 MB
        mock_psutil_proc2.cpu_percent.return_value = 5.2

        mock_psutil.Process.side_effect = [mock_psutil_proc1, mock_psutil_proc2]

        # Set last_stats_message_time to force collection
        self.supervisor._last_stats_message_time = datetime.min

        self.supervisor._collect_resource_stats()

        sup_id = self.supervisor.supervisor_id
        stats = self.supervisor.daq_job_stats

        self.assertAlmostEqual(
            stats[sup_id]["DigitizerJob"].resource_stats.rss_mb, 150.0, places=1
        )
        self.assertAlmostEqual(
            stats[sup_id]["DigitizerJob"].resource_stats.cpu_percent, 12.5
        )
        self.assertAlmostEqual(
            stats[sup_id]["StoreJob"].resource_stats.rss_mb, 80.0, places=1
        )
        self.assertAlmostEqual(
            stats[sup_id]["StoreJob"].resource_stats.cpu_percent, 5.2
        )

    @patch("enrgdaq.supervisor.psutil")
    def test_collect_resource_stats_skips_dead_processes(self, mock_psutil):
        """Dead processes should be skipped without error."""
        mock_proc = MagicMock()
        mock_proc.process.pid = 12345
        mock_proc.process.is_alive.return_value = False
        mock_proc.daq_job_cls.__name__ = "DeadJob"

        self.supervisor.daq_job_processes = [mock_proc]
        self.supervisor._last_stats_message_time = datetime.min

        # Should not raise
        self.supervisor._collect_resource_stats()

    @patch("enrgdaq.supervisor.psutil")
    def test_collect_resource_stats_handles_no_such_process(self, mock_psutil):
        """If a process disappears between is_alive and psutil.Process, handle gracefully."""
        mock_proc = MagicMock()
        mock_proc.process.pid = 99999
        mock_proc.process.is_alive.return_value = True
        mock_proc.daq_job_cls.__name__ = "GhostJob"

        self.supervisor.daq_job_processes = [mock_proc]
        mock_psutil.Process.side_effect = NoSuchProcess(99999)

        self.supervisor._last_stats_message_time = datetime.min

        # Should not raise
        self.supervisor._collect_resource_stats()

    @patch("enrgdaq.supervisor.psutil")
    def test_collect_resource_stats_caches_psutil_process(self, mock_psutil):
        """psutil.Process should be cached and reused for the same PID."""
        mock_proc = MagicMock()
        mock_proc.process.pid = 12345
        mock_proc.process.is_alive.return_value = True
        mock_proc.daq_job_cls.__name__ = "CachedJob"

        self.supervisor.daq_job_processes = [mock_proc]
        self.supervisor._last_stats_message_time = datetime.min

        mock_psutil_proc = MagicMock()
        mock_psutil_proc.memory_info.return_value.rss = 100 * 1024 * 1024
        mock_psutil_proc.cpu_percent.return_value = 10.0
        mock_psutil.Process.return_value = mock_psutil_proc

        # First call creates cache entry
        self.supervisor._collect_resource_stats()
        self.assertIn(12345, self.supervisor._psutil_process_cache)

        # Second call uses cache, psutil.Process should still have been called once
        self.supervisor._collect_resource_stats()
        mock_psutil.Process.assert_called_once_with(12345)

    # --- _collect_ring_buffer_stats ---

    @patch("enrgdaq.utils.shared_ring_buffer.get_global_ring_buffer")
    def test_collect_ring_buffer_stats_populates_stats(self, mock_get_rb):
        """Ring buffer stats should appear under 'RingBuffer' in daq_job_stats."""
        mock_rb = MagicMock()
        mock_rb.get_slot_occupancy.return_value = {
            "free": 10,
            "writing": 1,
            "ready": 2,
        }
        mock_rb.get_stats.return_value = (5000, 2000)
        mock_get_rb.return_value = mock_rb

        self.supervisor._collect_ring_buffer_stats()

        sup_id = self.supervisor.supervisor_id
        stats = self.supervisor.daq_job_stats[sup_id]["RingBuffer"]
        # ready_pct = 2 / 13 * 100 ≈ 15.38
        self.assertAlmostEqual(stats.resource_stats.cpu_percent, 15.38, places=1)
        # bytes_written in MB
        self.assertAlmostEqual(
            stats.resource_stats.rss_mb, 5000 / (1024 * 1024), places=4
        )

    @patch("enrgdaq.utils.shared_ring_buffer.get_global_ring_buffer")
    def test_collect_ring_buffer_stats_zero_total(self, mock_get_rb):
        """If total slots is 0, occupancy should not error."""
        mock_rb = MagicMock()
        mock_rb.get_slot_occupancy.return_value = {"free": 0, "writing": 0, "ready": 0}
        mock_rb.get_stats.return_value = (0, 0)
        mock_get_rb.return_value = mock_rb

        self.supervisor._collect_ring_buffer_stats()

        sup_id = self.supervisor.supervisor_id
        stats = self.supervisor.daq_job_stats[sup_id]["RingBuffer"]
        self.assertEqual(stats.resource_stats.cpu_percent, 0.0)

    @patch("enrgdaq.utils.shared_ring_buffer.get_global_ring_buffer")
    def test_collect_ring_buffer_stats_handles_exception(self, mock_get_rb):
        """If the ring buffer raises, _collect_ring_buffer_stats should not propagate."""
        mock_get_rb.side_effect = RuntimeError("Ring buffer not available")

        # Should not raise
        self.supervisor._collect_ring_buffer_stats()

    @patch("enrgdaq.supervisor.sys")
    def test_collect_ring_buffer_stats_skips_windows(self, mock_sys):
        """On Windows, ring buffer stats collection should be skipped."""
        mock_sys.platform = "win32"

        self.supervisor._collect_ring_buffer_stats()

        # daq_job_stats should be empty
        self.assertEqual(self.supervisor.daq_job_stats, {})


if __name__ == "__main__":
    unittest.main()
