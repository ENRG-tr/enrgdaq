import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from enrgdaq.daq.jobs.handle_stats import (
    DAQJobHandleStats,
    DAQJobHandleStatsConfig,
    DAQJobStatsRecord,
)
from enrgdaq.daq.jobs.remote import SupervisorRemoteStats
from enrgdaq.daq.models import (
    DAQJobInfo,
    DAQJobLatencyStats,
    DAQJobMessageStatsReport,
    DAQJobStats,
)
from enrgdaq.models import SupervisorInfo


class TestDAQJobHandleStats(unittest.TestCase):
    def setUp(self):
        self.config = DAQJobHandleStatsConfig(
            daq_job_type="",
            store_config=MagicMock(),
        )
        self.daq_job_handle_stats = DAQJobHandleStats(config=self.config)
        # Mock _publish_buffer instead of message_out
        self.daq_job_handle_stats._publish_buffer = MagicMock()

    def test_handle_message_success(self):
        """Test that DAQJobMessageStatsReport is handled correctly."""
        message = DAQJobMessageStatsReport(
            daq_job_info=DAQJobInfo(
                daq_job_type="DAQJobTest",
                unique_id="test-123",
                instance_id=0,
                config="",
                supervisor_info=SupervisorInfo(supervisor_id="test_supervisor"),
            ),
            processed_count=100,
            processed_bytes=10000,
            sent_count=50,
            sent_bytes=5000,
            latency=DAQJobLatencyStats(),
        )

        result = self.daq_job_handle_stats.handle_message(message)

        self.assertTrue(result)
        # Verify stats were stored
        self.assertIn("test_supervisor", self.daq_job_handle_stats._stats)
        self.assertIn("DAQJobTest", self.daq_job_handle_stats._stats["test_supervisor"])

    def test_handle_message_no_supervisor_info(self):
        """Test that message without supervisor info is skipped."""
        message = DAQJobMessageStatsReport(
            daq_job_info=None,
            processed_count=100,
            sent_count=50,
            latency=DAQJobLatencyStats(),
        )

        with patch(
            "enrgdaq.daq.jobs.handle_stats.DAQJob.handle_message", return_value=True
        ):
            result = self.daq_job_handle_stats.handle_message(message)

        self.assertTrue(result)
        # Stats should remain empty
        self.assertEqual(len(self.daq_job_handle_stats._stats), 0)

    def test_save_remote_stats(self):
        """Test that remote stats are saved correctly from _supervisor_activity."""
        # Add stats and activity data
        self.daq_job_handle_stats._stats = {
            "remote_1": {
                "DAQJobTest": DAQJobStats(
                    message_in_stats=DAQJobStatsRecord(count=10),
                    message_out_stats=DAQJobStatsRecord(count=5),
                )
            }
        }
        self.daq_job_handle_stats._supervisor_activity = {
            "remote_1": SupervisorRemoteStats(
                last_active=datetime.now() - timedelta(seconds=10),
                message_in_count=10,
                message_in_bytes=1000,
                message_out_count=5,
                message_out_bytes=500,
            )
        }
        self.daq_job_handle_stats._supervisor_info = SupervisorInfo(
            supervisor_id="local"
        )

        self.daq_job_handle_stats._save_remote_stats()
        self.daq_job_handle_stats._publish_buffer.put.assert_called_once()

    def test_save_remote_stats_empty(self):
        """Test that empty remote stats are handled."""
        self.daq_job_handle_stats._stats = {}
        self.daq_job_handle_stats._supervisor_activity = {}

        self.daq_job_handle_stats._save_remote_stats()
        self.daq_job_handle_stats._publish_buffer.put.assert_called_once()
        args, kwargs = self.daq_job_handle_stats._publish_buffer.put.call_args
        data = args[0].data

        self.assertEqual(len(data), 0)


if __name__ == "__main__":
    unittest.main()
