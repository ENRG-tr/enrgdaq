import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from enrgdaq.daq.jobs.handle_stats import (
    DAQJobHandleStats,
    DAQJobHandleStatsConfig,
    DAQJobMessageStats,
    DAQJobStatsRecord,
)
from enrgdaq.daq.jobs.test_job import DAQJobTest
from enrgdaq.daq.models import DAQJobStats


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

        result = self.daq_job_handle_stats.handle_message(message)

        self.assertTrue(result)
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


if __name__ == "__main__":
    unittest.main()
