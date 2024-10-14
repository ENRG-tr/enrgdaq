import unittest
from datetime import datetime
from typing import cast
from unittest.mock import MagicMock, patch

from daq.base import DAQJob
from daq.jobs.handle_stats import (
    DAQJobHandleStats,
    DAQJobMessageStats,
    DAQJobStatsRecord,
)
from daq.models import DAQJobStats


class TestDAQJobHandleStats(unittest.TestCase):
    def setUp(self):
        self.config = MagicMock()
        self.daq_job_handle_stats = DAQJobHandleStats(config=self.config)
        self.daq_job_handle_stats.message_out = MagicMock()

    def test_handle_message_success(self):
        message = DAQJobMessageStats(
            stats={
                type(cast(DAQJob, MagicMock())): DAQJobStats(
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
        message = MagicMock()
        with patch("daq.jobs.handle_stats.DAQJob.handle_message", return_value=False):
            result = self.daq_job_handle_stats.handle_message(message)

        self.assertFalse(result)
        self.daq_job_handle_stats.message_out.put.assert_not_called()


if __name__ == "__main__":
    unittest.main()
