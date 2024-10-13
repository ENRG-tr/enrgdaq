import unittest
from unittest.mock import MagicMock, patch

from N1081B import N1081B
from websocket import WebSocket

from daq.jobs.caen.n1081b import DAQJobN1081B, DAQJobN1081BConfig
from daq.store.models import DAQJobMessageStore


class TestDAQJobN1081B(unittest.TestCase):
    def setUp(self):
        self.config = DAQJobN1081BConfig(
            daq_job_type="n1081b",
            host="localhost",
            port="1234",
            password="password",
            sections_to_store=["SEC_A", "SEC_B"],
            store_config=MagicMock(),
        )
        self.daq_job = DAQJobN1081B(self.config)

    @patch.object(N1081B, "connect", return_value=True)
    @patch.object(N1081B, "login", return_value=True)
    def test_connect_to_device_success(self, mock_login, mock_connect):
        self.daq_job.device.ws = MagicMock(spec=WebSocket)
        self.daq_job._connect_to_device()
        mock_connect.assert_called_once()
        mock_login.assert_called_once_with("password")
        self.assertTrue(isinstance(self.daq_job.device.ws, WebSocket))

    @patch.object(N1081B, "connect", return_value=False)
    def test_connect_to_device_failure(self, mock_connect):
        with self.assertRaises(Exception) as context:
            self.daq_job._connect_to_device()
        self.assertTrue("Connection failed" in str(context.exception))

    @patch.object(N1081B, "login", return_value=False)
    @patch.object(N1081B, "connect", return_value=True)
    def test_login_failure(self, mock_connect, mock_login):
        with self.assertRaises(Exception) as context:
            self.daq_job._connect_to_device()
        self.assertTrue("Login failed" in str(context.exception))

    @patch.object(
        N1081B,
        "get_function_results",
        return_value={"data": {"counters": [{"lemo": 1, "value": 100}]}},
    )
    def test_poll_sections(self, mock_get_function_results):
        self.daq_job._send_store_message = MagicMock()
        self.daq_job._poll_sections()
        self.daq_job._send_store_message.assert_called()
        self.assertEqual(self.daq_job._send_store_message.call_count, 2)

    @patch.object(N1081B, "get_function_results", return_value={"data": {}})
    def test_poll_sections_no_counters(self, mock_get_function_results):
        self.daq_job._send_store_message = MagicMock()
        with self.assertRaises(Exception) as context:
            self.daq_job._poll_sections()
        self.assertTrue("No counters in section" in str(context.exception))
        self.daq_job._send_store_message.assert_not_called()

    @patch("time.sleep", return_value=None, side_effect=StopIteration)
    @patch.object(DAQJobN1081B, "_poll_sections")
    @patch.object(DAQJobN1081B, "_connect_to_device")
    @patch.object(DAQJobN1081B, "_is_connected", side_effect=[False, True])
    def test_start(
        self, mock_is_connected, mock_connect_to_device, mock_poll_sections, mock_sleep
    ):
        with self.assertRaises(StopIteration):
            self.daq_job.start()
        mock_connect_to_device.assert_called_once()
        mock_poll_sections.assert_called_once()

    @patch.object(N1081B, "get_function_results", return_value=None)
    def test_poll_sections_no_results(self, mock_get_function_results):
        self.daq_job._send_store_message = MagicMock()
        with self.assertRaises(Exception) as context:
            self.daq_job._poll_sections()
        self.assertTrue("No results" in str(context.exception))
        self.daq_job._send_store_message.assert_not_called()

    @patch.object(
        N1081B,
        "get_function_results",
        return_value={"data": {"counters": [{"lemo": 1, "value": 100}]}},
    )
    def test_send_store_message(self, mock_get_function_results):
        self.daq_job.message_out = MagicMock()
        self.daq_job._send_store_message(
            {"counters": [{"lemo": 1, "value": 100}]}, "SEC_A"
        )
        self.daq_job.message_out.put.assert_called_once()
        message = self.daq_job.message_out.put.call_args[0][0]
        self.assertIsInstance(message, DAQJobMessageStore)
        self.assertEqual(message.prefix, "SEC_A")
        self.assertIn("timestamp", message.keys)
        self.assertIn("lemo_1", message.keys)
        self.assertIn(100, message.data[0])

    def test_invalid_section_in_config(self):
        invalid_config = DAQJobN1081BConfig(
            daq_job_type="",
            host="localhost",
            port="1234",
            password="password",
            sections_to_store=["INVALID_SECTION"],
            store_config=MagicMock(),
        )
        with self.assertRaises(Exception) as context:
            DAQJobN1081B(invalid_config)
        self.assertTrue("Invalid section: INVALID_SECTION" in str(context.exception))

    @patch.object(N1081B, "connect", return_value=True)
    @patch.object(N1081B, "login", return_value=True)
    def test_connect_to_device_timeout(self, mock_login, mock_connect):
        self.daq_job.device.ws = MagicMock(spec=WebSocket)
        self.daq_job.device.ws.settimeout = MagicMock(side_effect=Exception("Timeout"))
        with self.assertRaises(Exception) as context:
            self.daq_job._connect_to_device()
        self.assertTrue("Timeout" in str(context.exception))
        mock_connect.assert_called_once()
        mock_login.assert_called_once_with("password")

    @patch.object(N1081B, "get_function_results", side_effect=Exception("Timeout"))
    def test_poll_sections_timeout(self, mock_get_function_results):
        self.daq_job._send_store_message = MagicMock()
        with self.assertRaises(Exception) as context:
            self.daq_job._poll_sections()
        self.assertTrue("Timeout" in str(context.exception))
        self.daq_job._send_store_message.assert_not_called()

    @patch.object(N1081B, "connect", return_value=True)
    @patch.object(N1081B, "login", return_value=True)
    def test_connect_to_device_no_websocket(self, mock_login, mock_connect):
        self.daq_job.device.ws = None  # type: ignore
        with self.assertRaises(Exception) as context:
            self.daq_job._connect_to_device()
        self.assertTrue("Websocket not found" in str(context.exception))
        mock_connect.assert_called_once()
        mock_login.assert_called_once_with("password")


if __name__ == "__main__":
    unittest.main()
