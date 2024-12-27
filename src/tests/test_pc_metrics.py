import unittest
from unittest.mock import MagicMock, patch

from enrgdaq.daq.jobs.pc_metrics import DAQJobPCMetrics, DAQJobPCMetricsConfig, PCMetric


class TestDAQJobPCMetrics(unittest.TestCase):
    def setUp(self):
        self.config = DAQJobPCMetricsConfig(
            daq_job_type=MagicMock(),
            store_config=MagicMock(),
            metrics_to_store=[PCMetric.CPU, PCMetric.MEMORY, PCMetric.DISK],
        )
        self.job = DAQJobPCMetrics(self.config)

    @patch("psutil.cpu_percent", return_value=50.0)
    @patch("psutil.virtual_memory")
    @patch("psutil.disk_usage")
    def test_poll_metrics(self, mock_disk_usage, mock_virtual_memory, mock_cpu_percent):
        mock_virtual_memory.return_value.percent = 75.0
        mock_disk_usage.return_value.percent = 60.0

        self.job._send_store_message = MagicMock()
        self.job._poll_metrics()

        expected_data = {
            PCMetric.CPU: 50.0,
            PCMetric.MEMORY: 75.0,
            PCMetric.DISK: 60.0,
        }
        self.job._send_store_message.assert_called_once_with(expected_data)

    @patch("time.sleep", return_value=None, side_effect=StopIteration)
    def test_start(self, mock_sleep):
        self.job.consume = MagicMock()
        self.job._poll_metrics = MagicMock()

        with self.assertRaises(StopIteration):
            self.job.start()

        self.job._poll_metrics.assert_called()


if __name__ == "__main__":
    unittest.main()
