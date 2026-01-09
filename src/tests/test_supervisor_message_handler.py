"""Tests for SupervisorMessageHandler."""

import unittest
from unittest.mock import MagicMock, patch

from enrgdaq.daq.models import DAQJobInfo, DAQJobLatencyStats, DAQJobMessageStatsReport
from enrgdaq.models import SupervisorInfo
from enrgdaq.supervisor_message_handler import SupervisorMessageHandler


class TestSupervisorMessageHandler(unittest.TestCase):
    """Test cases for SupervisorMessageHandler."""

    def test_init(self):
        """Test handler initialization."""
        handler = SupervisorMessageHandler(
            xpub_url="ipc:///tmp/test_xpub.ipc",
            supervisor_id="test_supervisor",
            on_stats_report=None,
            on_remote_stats=None,
        )

        self.assertEqual(handler._supervisor_id, "test_supervisor")
        self.assertEqual(handler._xpub_url, "ipc:///tmp/test_xpub.ipc")
        self.assertFalse(handler._is_stopped)

    def test_stats_topic_property(self):
        """Test that stats_topic returns the correct topic string."""
        handler = SupervisorMessageHandler(
            xpub_url="ipc:///tmp/test.ipc",
            supervisor_id="my_supervisor",
        )

        # New topic format: stats.{supervisor_id}
        self.assertEqual(handler.stats_topic, "stats.my_supervisor")

    def test_handle_stats_report_callback(self):
        """Test that on_stats_report callback is invoked correctly."""
        callback = MagicMock()
        handler = SupervisorMessageHandler(
            xpub_url="ipc:///tmp/test.ipc",
            supervisor_id="test",
            on_stats_report=callback,
        )

        # Create a mock stats report
        msg = DAQJobMessageStatsReport(
            processed_count=100,
            sent_count=50,
            latency=DAQJobLatencyStats(
                count=100, min_ms=1.0, max_ms=10.0, avg_ms=5.0, p95_ms=8.0, p99_ms=9.0
            ),
            daq_job_info=DAQJobInfo(
                daq_job_type="TestJob",
                unique_id="test-123",
                instance_id=0,
                config="",
                supervisor_info=SupervisorInfo(supervisor_id="test"),
            ),
        )

        # Directly call internal method to test callback
        if handler._on_stats_report:
            handler._on_stats_report(msg)

        callback.assert_called_once_with(msg)

    def test_stop_idempotent(self):
        """Test that stop() can be called multiple times without error."""
        handler = SupervisorMessageHandler(
            xpub_url="ipc:///tmp/test.ipc",
            supervisor_id="test",
        )

        # Should not raise even when not started
        handler.stop()
        handler.stop()  # Second call should also be safe

        self.assertTrue(handler._is_stopped)


class TestSupervisorStatsHandling(unittest.TestCase):
    """Test stats handling integration in Supervisor."""

    @patch("enrgdaq.supervisor.logging")
    def test_handle_stats_report(self, mock_logging):
        """Test Supervisor._handle_stats_report updates stats correctly."""
        from enrgdaq.models import SupervisorConfig, SupervisorInfo
        from enrgdaq.supervisor import Supervisor

        supervisor = Supervisor()
        supervisor.config = SupervisorConfig(
            info=SupervisorInfo(supervisor_id="test"), cnc=None
        )
        supervisor.daq_job_stats = {}
        supervisor._logger = MagicMock()

        msg = DAQJobMessageStatsReport(
            processed_count=100,
            sent_count=50,
            latency=DAQJobLatencyStats(
                count=100, min_ms=1.0, max_ms=10.0, avg_ms=5.0, p95_ms=8.0, p99_ms=9.0
            ),
            daq_job_info=DAQJobInfo(
                daq_job_type="DAQJobTest",
                unique_id="test-123",
                instance_id=0,
                config="",
                supervisor_info=SupervisorInfo(supervisor_id="test"),
            ),
        )

        supervisor._handle_stats_report(msg)

        # Check that stats were created and updated
        self.assertIn("DAQJobTest", supervisor.daq_job_stats)
        stats = supervisor.daq_job_stats["DAQJobTest"]
        self.assertEqual(stats.message_in_stats.count, 100)
        self.assertEqual(stats.message_out_stats.count, 50)
        self.assertEqual(stats.latency_stats.avg_ms, 5.0)

    @patch("enrgdaq.supervisor.logging")
    def test_handle_stats_report_no_daq_job_info(self, mock_logging):
        """Test that _handle_stats_report handles missing daq_job_info gracefully."""
        from enrgdaq.models import SupervisorConfig, SupervisorInfo
        from enrgdaq.supervisor import Supervisor

        supervisor = Supervisor()
        supervisor.config = SupervisorConfig(
            info=SupervisorInfo(supervisor_id="test"), cnc=None
        )
        supervisor.daq_job_stats = {}
        supervisor._logger = MagicMock()

        msg = DAQJobMessageStatsReport(
            processed_count=100,
            sent_count=50,
            latency=DAQJobLatencyStats(),
            daq_job_info=None,  # No daq_job_info
        )

        # Should not raise
        supervisor._handle_stats_report(msg)

        # Stats should remain empty
        self.assertEqual(len(supervisor.daq_job_stats), 0)


class TestDAQJobMessageStatsReportTopic(unittest.TestCase):
    """Test DAQJobMessageStatsReport topic behavior."""

    def test_pre_send_adds_stats_topic(self):
        """Test that pre_send adds the stats-specific topic."""
        msg = DAQJobMessageStatsReport(
            processed_count=10,
            sent_count=5,
            latency=DAQJobLatencyStats(),
            daq_job_info=DAQJobInfo(
                daq_job_type="TestJob",
                unique_id="test-123",
                instance_id=0,
                config="",
                supervisor_info=SupervisorInfo(supervisor_id="my_supervisor"),
            ),
        )

        msg.pre_send()

        # New topic format: stats.{supervisor_id}
        self.assertIn("stats.my_supervisor", msg.topics)

    def test_pre_send_always_adds_stats_topic(self):
        """Test that pre_send always adds stats topic regardless of target_supervisor."""
        msg = DAQJobMessageStatsReport(
            processed_count=10,
            sent_count=5,
            latency=DAQJobLatencyStats(),
            target_supervisor=False,
            daq_job_info=DAQJobInfo(
                daq_job_type="TestJob",
                unique_id="test-123",
                instance_id=0,
                config="",
                supervisor_info=SupervisorInfo(supervisor_id="my_supervisor"),
            ),
        )

        msg.pre_send()

        # Stats topic is always added
        self.assertIn("stats.my_supervisor", msg.topics)


if __name__ == "__main__":
    unittest.main()
