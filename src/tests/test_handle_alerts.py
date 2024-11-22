import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from enrgdaq.daq.alert.base import DAQJobMessageAlert
from enrgdaq.daq.jobs.handle_alerts import DAQJobHandleAlerts, DAQJobHandleAlertsConfig
from enrgdaq.daq.store.models import DAQJobMessageStore
from enrgdaq.utils.time import get_unix_timestamp_ms


class TestDAQJobHandleAlerts(unittest.TestCase):
    def setUp(self):
        self.config = DAQJobHandleAlertsConfig(
            daq_job_type="", store_config=MagicMock()
        )
        self.daq_job = DAQJobHandleAlerts(config=self.config)
        self.daq_job.message_out = MagicMock()

    @patch("enrgdaq.utils.time.get_unix_timestamp_ms", return_value=1234567890)
    def test_handle_message(self, mock_get_unix_timestamp_ms):
        date = datetime(2022, 1, 1, 0, 0, 0)

        message = MagicMock(spec=DAQJobMessageAlert)
        message.date = date
        message.daq_job_info = MagicMock()
        message.alert_info = MagicMock()
        message.alert_info.severity = "high"
        message.alert_info.message = "Test alert message"

        result = self.daq_job.handle_message(message)

        self.assertTrue(result)
        self.daq_job.message_out.put.assert_called_once()
        args, kwargs = self.daq_job.message_out.put.call_args
        stored_message = args[0]
        self.assertIsInstance(stored_message, DAQJobMessageStore)
        self.assertEqual(
            stored_message.keys, ["timestamp", "daq_job", "severity", "message"]
        )
        self.assertEqual(
            stored_message.data,
            [
                [
                    get_unix_timestamp_ms(date),
                    message.daq_job_info.daq_job_class_name,
                    "high",
                    "Test alert message",
                ]
            ],
        )

    def test_handle_message_invalid(self):
        message = MagicMock()
        result = None

        with self.assertRaises(Exception) as context:
            result = self.daq_job.handle_message(message)

        self.assertIn(
            "is not accepted by",
            str(context.exception),
        )

        self.assertFalse(result)
        self.daq_job.message_out.put.assert_not_called()

    def test_start(self):
        self.daq_job.consume = MagicMock(side_effect=[None, Exception("Stop")])

        with self.assertRaises(Exception) as context:
            self.daq_job.start()

        self.assertEqual(str(context.exception), "Stop")
        self.daq_job.consume.assert_called()


if __name__ == "__main__":
    unittest.main()
