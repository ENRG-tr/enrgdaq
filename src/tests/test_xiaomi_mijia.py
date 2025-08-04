import unittest
from unittest.mock import MagicMock, patch

from enrgdaq.daq.jobs.sensor.xiaomi_mijia import (
    DAQJobXiaomiMijia,
    DAQXiaomiMijiaConfig,
)


class DummyData:
    def __init__(self, temperature=22.5, humidity=55.0, battery=90):
        self.temperature = temperature
        self.humidity = humidity
        self.battery = battery


class TestDAQJobXiaomiMijia(unittest.TestCase):
    def setUp(self):
        self.config = DAQXiaomiMijiaConfig(
            daq_job_type="DAQJobXiaomiMijia",
            mac_address="AA:BB:CC:DD:EE:FF",
            poll_interval_seconds=1,
            connect_retries=2,
            connect_retry_delay=0.1,
            store_config=MagicMock(),
        )
        self.job = DAQJobXiaomiMijia(self.config)
        self.job._logger = MagicMock()
        self.job._put_message_out = MagicMock()

    @patch("enrgdaq.daq.jobs.sensor.xiaomi_mijia.Lywsd03mmcClient")
    def test_connect_with_retries_success(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.data = DummyData()
        mock_client_cls.return_value = mock_client

        self.job._get_data()
        self.assertIs(self.job._client, mock_client)
        self.job._logger.info.assert_any_call("Connected to sensor.")

    @patch("enrgdaq.daq.jobs.sensor.xiaomi_mijia.Lywsd03mmcClient")
    @patch("time.sleep", return_value=None)
    def test_connect_with_retries_fail_then_success(self, mock_sleep, mock_client_cls):
        # First attempt fails, second succeeds
        fail_once = False

        def side_effect(*args, **kwargs):
            nonlocal fail_once
            if not hasattr(self, "_fail_once"):
                fail_once = True
                raise Exception("fail")
            return MagicMock(data=DummyData())

        mock_client_cls.side_effect = side_effect

        self.job._get_data()
        self.assertTrue(self.job._logger.warning.called)
        self.assertTrue(self.job._logger.info.called)

    @patch("enrgdaq.daq.jobs.sensor.xiaomi_mijia.Lywsd03mmcClient")
    @patch("time.sleep", return_value=None)
    def test_connect_with_retries_all_fail(self, mock_sleep, mock_client_cls):
        mock_client_cls.side_effect = Exception("fail")
        with self.assertRaises(Exception):
            self.job._get_data()
        self.assertTrue(self.job._logger.error.called)

    @patch(
        "enrgdaq.daq.jobs.sensor.xiaomi_mijia.get_now_unix_timestamp_ms",
        return_value=123456789,
    )
    def test_send_store_message(self, mock_time):
        self.job._client = MagicMock(units="C")
        data = DummyData(temperature=23.1, humidity=60.2, battery=88)
        self.job._send_store_message(data)
        args, kwargs = self.job._put_message_out.call_args
        msg = args[0]
        self.assertEqual(msg.keys, ["timestamp", "temperature", "humidity", "battery"])
        self.assertEqual(msg.data[0][:4], [123456789, 23.1, 60.2, 88])

    @patch(
        "enrgdaq.daq.jobs.sensor.xiaomi_mijia.DAQJobXiaomiMijia._connect_with_retries"
    )
    @patch("enrgdaq.daq.jobs.sensor.xiaomi_mijia.DAQJobXiaomiMijia._send_store_message")
    @patch("time.sleep", return_value=None)
    def test_start_successful_loop(self, mock_sleep, mock_send_store, mock_connect):
        self.job._client = MagicMock()
        self.job._client.data = DummyData()
        self.job.consume = MagicMock()
        called = False

        # Stop after 2 iterations
        def side_effect():
            nonlocal called
            if not called:
                called = True
            else:
                raise KeyboardInterrupt()

        self.job.consume.side_effect = side_effect

        with self.assertRaises(KeyboardInterrupt):
            self.job.start()
        self.assertTrue(mock_connect.called)
        self.assertTrue(mock_send_store.called)

    @patch(
        "enrgdaq.daq.jobs.sensor.xiaomi_mijia.DAQJobXiaomiMijia._connect_with_retries"
    )
    @patch("time.sleep", return_value=None)
    def test_start_handles_data_exception(self, mock_sleep, mock_connect):
        self.job._client = MagicMock()
        self.job._client.data = MagicMock(side_effect=Exception("fail"))
        self.job.consume = MagicMock(side_effect=Exception)
        self.job._send_store_message = MagicMock()

        with self.assertRaises(Exception):
            self.job.start()
        self.assertTrue(mock_connect.called)


if __name__ == "__main__":
    unittest.main()
