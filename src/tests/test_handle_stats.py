import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from enrgdaq.daq.jobs.handle_stats import (
    DAQJobHandleStats,
    DAQJobHandleStatsConfig,
    DAQJobMessageStats,
    DAQJobStatsRecord,
)
from enrgdaq.daq.jobs.remote import SupervisorRemoteStats
from enrgdaq.daq.jobs.test_job import DAQJobTest
from enrgdaq.daq.models import DAQJobInfo, DAQJobStats
from enrgdaq.models import SupervisorConfig


class TestDAQJobHandleStats(unittest.TestCase):
    def setUp(self):
        self.config = DAQJobHandleStatsConfig(
            daq_job_type="",
            store_config=MagicMock(),
        )
        self.daq_job_handle_stats = DAQJobHandleStats(config=self.config)
        self.daq_job_handle_stats.message_out = MagicMock()

    def test_handle_message_success(self):
        message = DAQJobMessageStats(
            daq_job_info=DAQJobInfo.mock(),
            stats={
                DAQJobTest: DAQJobStats(
                    message_in_stats=DAQJobStatsRecord(
                        last_updated=datetime.now(), count=5
                    ),
                    message_out_stats=DAQJobStatsRecord(
                        last_updated=datetime.now(), count=3
                    ),
                    restart_stats=DAQJobStatsRecord(
                        last_updated=datetime.now(), count=1
                    ),
                )
            },
        )

        result = self.daq_job_handle_stats.handle_message(message)

        self.assertTrue(result)
        self.daq_job_handle_stats._save_stats()
        self.daq_job_handle_stats.message_out.put.assert_called_once()

    def test_handle_message_failure(self):
        message = DAQJobMessageStats(
            stats={
                DAQJobTest: DAQJobStats(
                    message_in_stats=DAQJobStatsRecord(
                        last_updated=datetime.now(), count=5
                    ),
                    message_out_stats=DAQJobStatsRecord(
                        last_updated=datetime.now(), count=3
                    ),
                    restart_stats=DAQJobStatsRecord(
                        last_updated=datetime.now(), count=1
                    ),
                )
            }
        )
        with patch(
            "enrgdaq.daq.jobs.handle_stats.DAQJob.handle_message", return_value=False
        ):
            result = self.daq_job_handle_stats.handle_message(message)

        self.assertFalse(result)
        self.daq_job_handle_stats.message_out.put.assert_not_called()

    def test_save_remote_stats(self):
        self.daq_job_handle_stats._remote_stats = {
            "remote_1": {
                "remote_1": SupervisorRemoteStats(
                    last_active=datetime.now() - timedelta(seconds=10),
                    message_in_count=10,
                    message_in_bytes=1000,
                    message_out_count=5,
                    message_out_bytes=500,
                )
            },
            "remote_2": {
                "remote_2": SupervisorRemoteStats(
                    last_active=datetime.now() - timedelta(seconds=40),
                    message_in_count=20,
                    message_in_bytes=2000,
                    message_out_count=10,
                    message_out_bytes=1000,
                )
            },
        }

        self.daq_job_handle_stats._supervisor_config = SupervisorConfig(
            supervisor_id="remote_1"
        )
        self.daq_job_handle_stats._save_remote_stats()
        self.daq_job_handle_stats.message_out.put.assert_called_once()
        args, kwargs = self.daq_job_handle_stats.message_out.put.call_args
        data = args[0].data

        self.assertEqual(len(data), 2)
        self.assertEqual(data[0][0], "remote_1")
        self.assertEqual(data[0][1], "true")
        self.assertEqual(data[1][0], "remote_2")
        self.assertEqual(data[1][1], "false")

    def test_save_remote_stats_empty(self):
        self.daq_job_handle_stats._remote_stats = {}

        self.daq_job_handle_stats._save_remote_stats()
        self.daq_job_handle_stats.message_out.put.assert_called_once()
        args, kwargs = self.daq_job_handle_stats.message_out.put.call_args
        data = args[0].data

        self.assertEqual(len(data), 0)


if __name__ == "__main__":
    unittest.main()
