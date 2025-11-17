import unittest
from unittest.mock import MagicMock, patch

from enrgdaq.daq.jobs.sensor.xiaomi_mijia import (
    DAQJobXiaomiMijia,
    DAQXiaomiMijiaConfig,
    DAQJobMessageStoreTabular, 
    get_now_unix_timestamp_ms, 
)


class DummyData:
    """Mock for Lywsd03mmcData"""
    def __init__(self, temperature=22.5, hum=55.0, battery_percentage=90):
        self.temperature = temperature
        self.hum = hum
        self.battery_percentage = battery_percentage


class TestDAQJobXiaomiMijia(unittest.TestCase):
    def setUp(self):
        self.config = DAQXiaomiMijiaConfig(
            daq_job_type="DAQJobXiaomiMijia",
            mac_address="AA:BB:CC:DD:EE:FF",
            poll_interval_seconds=1,
            connect_retries=2,
            connect_retry_delay=0.1,
            timeout_sec=5.0,  # Added new config property
            store_config=MagicMock(),
        )
        self.job = DAQJobXiaomiMijia(self.config)
        self.job._logger = MagicMock()
        self.job._put_message_out = MagicMock()

    @patch("enrgdaq.daq.jobs.sensor.xiaomi_mijia.Lywsd03mmcClientSyncContext")
    def test_connect_with_retries_success(self, mock_client_context_cls):
        # 1. Setup mock client (returned by __enter__)
        mock_client = MagicMock()
        mock_data = DummyData()
        mock_client.get_data.return_value = mock_data
        mock_client.units = "C"

        # 2. Setup mock context manager (returned by the class)
        mock_context_manager = MagicMock()
        mock_context_manager.__enter__.return_value = mock_client
        mock_client_context_cls.return_value = mock_context_manager

        # 3. Run test
        result_data= self.job._get_data()

        # 4. Assert
        self.assertIs(result_data, mock_data)
        self.job._logger.debug.assert_any_call("Connected and got data.")
        # Check if context manager was called with correct args
        mock_client_context_cls.assert_called_with(
            self.config.mac_address, timeout_sec=self.config.timeout_sec
        )

    @patch("enrgdaq.daq.jobs.sensor.xiaomi_mijia.Lywsd03mmcClientSyncContext")
    @patch("time.sleep", return_value=None)
    def test_connect_with_retries_fail_then_success(self, mock_sleep, mock_client_context_cls):
        # 1. Setup successful mock client and context
        mock_client = MagicMock()
        mock_data = DummyData()
        mock_client.get_data.return_value = mock_data
        mock_client.units = "F"
        
        mock_context_manager = MagicMock()
        mock_context_manager.__enter__.return_value = mock_client

        # 2. Setup side effect: fail first, then succeed
        mock_client_context_cls.side_effect = [
            Exception("fail"),
            mock_context_manager,
        ]
        
        # 3. Run test
        result_data= self.job._get_data()

        # 4. Assert
        self.assertIs(result_data, mock_data)
        self.assertTrue(self.job._logger.warning.called) # Logged the failure
        self.job._logger.debug.assert_any_call("Connected and got data.") # Logged success
        self.assertEqual(mock_client_context_cls.call_count, 2)
        mock_sleep.assert_called_once_with(self.config.connect_retry_delay)

    @patch("enrgdaq.daq.jobs.sensor.xiaomi_mijia.Lywsd03mmcClientSyncContext")
    @patch("time.sleep", return_value=None)
    def test_connect_with_retries_all_fail(self, mock_sleep, mock_client_context_cls):
        # 1. Setup side effect: always fail
        mock_client_context_cls.side_effect = Exception("fail")
        
        # 2. Run test & assert
        with self.assertRaises(Exception):
            self.job._get_data()
            
        self.assertTrue(self.job._logger.error.called)
        self.assertEqual(mock_client_context_cls.call_count, self.config.connect_retries)
        self.assertEqual(mock_sleep.call_count, self.config.connect_retries - 1)

    @patch(
        "enrgdaq.daq.jobs.sensor.xiaomi_mijia.get_now_unix_timestamp_ms",
        return_value=123456789,
    )
    def test_send_store_message(self, mock_time):
        # 1. Setup
        data = DummyData(temperature=23.1, hum=60.2, battery_percentage=88)
        units = "C"

        # 2. Run test
        self.job._send_store_message(data)
        
        # 3. Assert
        args, kwargs = self.job._put_message_out.call_args
        msg = args[0]
        
        self.assertIsInstance(msg, DAQJobMessageStoreTabular)
        # Check keys
        self.assertEqual(msg.keys, ["timestamp", "temperature", "humidity", "battery"])
        # Check data
        expected_data = [123456789, 23.1, 60.2, 88]
        self.assertEqual(msg.data[0], expected_data)
        
        self.job._logger.debug.assert_called_with(
            f"Sending data to store: {dict(zip(msg.keys, expected_data))}"
        )

    @patch("enrgdaq.daq.jobs.sensor.xiaomi_mijia.DAQJobXiaomiMijia._get_data")
    @patch("enrgdaq.daq.jobs.sensor.xiaomi_mijia.DAQJobXiaomiMijia._send_store_message")
    @patch("time.sleep", return_value=None)
    def test_start_successful_loop(self, mock_sleep, mock_send_store, mock_get_data):
        # 1. Setup
        mock_data = DummyData()
        mock_get_data.return_value = (mock_data)
        
        self.job.consume = MagicMock()

        # 2. Setup loop break
        called = False
        def side_effect():
            nonlocal called
            if not called:
                called = True
            else:
                raise KeyboardInterrupt() # Stop after 2nd iteration

        self.job.consume.side_effect = side_effect

        # 3. Run test
        with self.assertRaises(KeyboardInterrupt):
            self.job.start()
            
        # 4. Assert
        self.assertEqual(self.job.consume.call_count, 2)
        self.assertEqual(mock_get_data.call_count, 1)
        self.assertEqual(mock_send_store.call_count, 1)
        # Check that send_store was called with the correct args
        mock_send_store.assert_called_with(mock_data)

    @patch("enrgdaq.daq.jobs.sensor.xiaomi_mijia.DAQJobXiaomiMijia._get_data")
    @patch("time.sleep", return_value=None)
    def test_start_handles_data_exception(self, mock_sleep, mock_get_data):
        # 1. Setup
        mock_get_data.side_effect = Exception("fail")
        self.job._send_store_message = MagicMock()
        
        # 2. Setup loop break
        def fail_on_second_call(method):
            if method.call_count == 2:
                raise Exception("Stop loop")

        self.job.consume = MagicMock(side_effect=lambda: fail_on_second_call(self.job.consume))

        # 3. Run test
        with self.assertRaises(Exception, msg="Stop loop"):
            self.job.start()
            
        # 4. Assert
        self.assertEqual(self.job.consume.call_count, 2)
        self.assertEqual(mock_get_data.call_count, 1)
        self.job._logger.warning.assert_any_call("Failed to get data: fail. Retrying after poll interval...")
        self.assertFalse(self.job._send_store_message.called) 
        self.assertEqual(mock_sleep.call_count, 1) 


if __name__ == "__main__":
    unittest.main()