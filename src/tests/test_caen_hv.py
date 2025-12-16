import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from enrgdaq.daq.jobs.caen.hv import (
    DAQJobCAENHV,
    DAQJobCAENHVConfig,
    DAQJobMessageCAENHVSetChParam,
)
from enrgdaq.daq.store.models import DAQJobMessageStore, DAQJobStoreConfig


class MockBoard:
    """Mock board object returned by get_crate_map()."""

    def __init__(self, slot: int, n_channel: int):
        self.slot = slot
        self.n_channel = n_channel


class MockParamProp:
    """Mock parameter property object."""

    def __init__(self, mode, param_type):
        self.mode = mode
        self.type = SimpleNamespace(name=param_type)


class TestDAQJobCAENHVConfig(unittest.TestCase):
    def test_config_creation(self):
        """Test that config can be created with required fields."""
        config = DAQJobCAENHVConfig(
            daq_job_type="DAQJobCAENHV",
            system_type="SY4527",
            link_type="TCPIP",
            connection_arg="192.168.1.100",
        )
        self.assertEqual(config.system_type, "SY4527")
        self.assertEqual(config.link_type, "TCPIP")
        self.assertEqual(config.connection_arg, "192.168.1.100")
        self.assertEqual(config.username, "")
        self.assertEqual(config.password, "")
        self.assertEqual(config.poll_interval_seconds, 1)
        self.assertIsNone(config.store_config)

    def test_config_with_optional_fields(self):
        """Test config with optional fields specified."""
        store_config = MagicMock(spec=DAQJobStoreConfig)
        config = DAQJobCAENHVConfig(
            daq_job_type="DAQJobCAENHV",
            system_type="N1470",
            link_type="USB",
            connection_arg="/dev/ttyUSB0",
            username="admin",
            password="secret",
            poll_interval_seconds=2.5,
            store_config=store_config,
            params_to_monitor=["VMon", "IMon", "V0Set"],
            channels_to_monitor=[{"slot": 0, "channels": [0, 1]}],
        )
        self.assertEqual(config.username, "admin")
        self.assertEqual(config.password, "secret")
        self.assertEqual(config.poll_interval_seconds, 2.5)
        self.assertEqual(config.params_to_monitor, ["VMon", "IMon", "V0Set"])
        self.assertEqual(config.channels_to_monitor, [{"slot": 0, "channels": [0, 1]}])


class TestDAQJobMessageCAENHVSetChParam(unittest.TestCase):
    def test_message_creation(self):
        """Test that the set channel param message can be created."""
        msg = DAQJobMessageCAENHVSetChParam(
            slot=0,
            channel_list=[0, 1, 2],
            param_name="V0Set",
            value=500.0,
        )
        self.assertEqual(msg.slot, 0)
        self.assertEqual(msg.channel_list, [0, 1, 2])
        self.assertEqual(msg.param_name, "V0Set")
        self.assertEqual(msg.value, 500.0)

    def test_message_with_int_value(self):
        """Test message with integer value (e.g., Pw on/off)."""
        msg = DAQJobMessageCAENHVSetChParam(
            slot=1,
            channel_list=[0],
            param_name="Pw",
            value=1,
        )
        self.assertEqual(msg.param_name, "Pw")
        self.assertEqual(msg.value, 1)

    def test_message_with_string_value(self):
        """Test message with string value."""
        msg = DAQJobMessageCAENHVSetChParam(
            slot=0,
            channel_list=[0],
            param_name="SomeParam",
            value="test_value",
        )
        self.assertEqual(msg.value, "test_value")


class TestDAQJobCAENHV(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.store_config = MagicMock(spec=DAQJobStoreConfig)
        self.config = DAQJobCAENHVConfig(
            daq_job_type="DAQJobCAENHV",
            system_type="SY4527",
            link_type="TCPIP",
            connection_arg="192.168.1.100",
            store_config=self.store_config,
        )

    def test_init(self):
        """Test DAQJobCAENHV initialization."""
        daq_job = DAQJobCAENHV(self.config)
        self.assertEqual(daq_job.config, self.config)
        self.assertIsNone(daq_job._device)
        self.assertEqual(
            daq_job.allowed_message_in_types, [DAQJobMessageCAENHVSetChParam]
        )

    @patch("enrgdaq.daq.jobs.caen.hv.hv")
    def test_start_connection_error(self, mock_hv):
        """Test that connection errors are properly raised."""
        mock_hv.SystemType = {"SY4527": "SY4527"}
        mock_hv.LinkType = {"TCPIP": "TCPIP"}
        mock_hv.Device.open.side_effect = Exception("Connection refused")

        daq_job = DAQJobCAENHV(self.config)
        with self.assertRaises(Exception) as context:
            daq_job.start()
        self.assertTrue("Connection refused" in str(context.exception))

    @patch("enrgdaq.daq.jobs.caen.hv.hv")
    @patch("enrgdaq.daq.jobs.caen.hv.sleep_for", side_effect=StopIteration)
    def test_start_success(self, mock_sleep_for, mock_hv):
        """Test successful start and loop."""
        # Setup mocks
        mock_device = MagicMock()
        mock_device.get_crate_map.return_value = [MockBoard(0, 2)]
        mock_hv.SystemType = {"SY4527": "SY4527"}
        mock_hv.LinkType = {"TCPIP": "TCPIP"}
        mock_hv.lib.sw_release.return_value = "1.0.0"
        mock_hv.Device.open.return_value.__enter__ = MagicMock(return_value=mock_device)
        mock_hv.Device.open.return_value.__exit__ = MagicMock(return_value=False)

        daq_job = DAQJobCAENHV(self.config)
        with self.assertRaises(StopIteration):
            daq_job.start()

        mock_device.get_crate_map.assert_called_once()

    def test_poll_channel_params_no_store_config(self):
        """Test that polling does nothing when store_config is None."""
        config = DAQJobCAENHVConfig(
            daq_job_type="DAQJobCAENHV",
            system_type="SY4527",
            link_type="TCPIP",
            connection_arg="192.168.1.100",
            store_config=None,
        )
        daq_job = DAQJobCAENHV(config)
        mock_device = MagicMock()
        slots = [MockBoard(0, 2)]

        # Should not raise and should not send any messages
        daq_job._poll_channel_params(mock_device, slots)
        mock_device.get_ch_param_info.assert_not_called()

    @patch("enrgdaq.daq.jobs.caen.hv.hv")
    @patch(
        "enrgdaq.daq.jobs.caen.hv.get_now_unix_timestamp_ms", return_value=1234567890
    )
    def test_poll_channel_params_success(self, mock_timestamp, mock_hv):
        """Test successful polling of channel parameters."""
        # Create mock param mode enum
        mock_hv.ParamMode.WRONLY = "WRONLY"

        mock_device = MagicMock()
        mock_device.get_ch_param_info.return_value = ["VMon", "IMon"]
        mock_device.get_ch_param_prop.return_value = MockParamProp(
            mode="RDONLY", param_type="FLOAT"
        )
        mock_device.get_ch_param.return_value = [100.5]

        slots = [MockBoard(0, 2)]

        daq_job = DAQJobCAENHV(self.config)
        daq_job._put_message_out = MagicMock()

        daq_job._poll_channel_params(mock_device, slots)

        # Should have called put_message_out once with tabular data
        daq_job._put_message_out.assert_called_once()
        message = daq_job._put_message_out.call_args[0][0]
        self.assertIsInstance(message, DAQJobMessageStore)
        self.assertEqual(message.tag, "ch_param")
        self.assertIn("timestamp", message.keys)
        self.assertIn("slot", message.keys)
        self.assertIn("channel", message.keys)
        self.assertIn("param_name", message.keys)
        self.assertIn("value", message.keys)

    @patch("enrgdaq.daq.jobs.caen.hv.hv")
    def test_poll_channel_params_skip_wronly(self, mock_hv):
        """Test that write-only parameters are skipped during polling."""
        mock_hv.ParamMode.WRONLY = "WRONLY"

        mock_device = MagicMock()
        mock_device.get_ch_param_info.return_value = ["V0Set"]
        mock_device.get_ch_param_prop.return_value = MockParamProp(
            mode="WRONLY", param_type="FLOAT"
        )

        slots = [MockBoard(0, 1)]

        daq_job = DAQJobCAENHV(self.config)
        daq_job._put_message_out = MagicMock()

        daq_job._poll_channel_params(mock_device, slots)

        # Should not send any message since the only param is write-only
        daq_job._put_message_out.assert_not_called()

    @patch("enrgdaq.daq.jobs.caen.hv.hv")
    def test_poll_channel_params_with_filter(self, mock_hv):
        """Test polling with params_to_monitor filter."""
        mock_hv.ParamMode.WRONLY = "WRONLY"

        config = DAQJobCAENHVConfig(
            daq_job_type="DAQJobCAENHV",
            system_type="SY4527",
            link_type="TCPIP",
            connection_arg="192.168.1.100",
            store_config=self.store_config,
            params_to_monitor=["VMon"],  # Only monitor VMon
        )

        mock_device = MagicMock()
        mock_device.get_ch_param_info.return_value = ["VMon", "IMon", "Status"]
        mock_device.get_ch_param_prop.return_value = MockParamProp(
            mode="RDONLY", param_type="FLOAT"
        )
        mock_device.get_ch_param.return_value = [100.0]

        slots = [MockBoard(0, 1)]

        daq_job = DAQJobCAENHV(config)
        daq_job._put_message_out = MagicMock()

        daq_job._poll_channel_params(mock_device, slots)

        # Should only poll VMon, not IMon or Status
        # get_ch_param_prop should only be called for VMon
        self.assertEqual(mock_device.get_ch_param_prop.call_count, 1)

    @patch("enrgdaq.daq.jobs.caen.hv.hv")
    def test_poll_channel_params_with_channel_filter(self, mock_hv):
        """Test polling with channels_to_monitor filter."""
        mock_hv.ParamMode.WRONLY = "WRONLY"

        config = DAQJobCAENHVConfig(
            daq_job_type="DAQJobCAENHV",
            system_type="SY4527",
            link_type="TCPIP",
            connection_arg="192.168.1.100",
            store_config=self.store_config,
            channels_to_monitor=[{"slot": 0, "channels": [1]}],  # Only channel 1
        )

        mock_device = MagicMock()
        mock_device.get_ch_param_info.return_value = ["VMon"]
        mock_device.get_ch_param_prop.return_value = MockParamProp(
            mode="RDONLY", param_type="FLOAT"
        )
        mock_device.get_ch_param.return_value = [100.0]

        # Board with 4 channels, but we only monitor channel 1
        slots = [MockBoard(0, 4)]

        daq_job = DAQJobCAENHV(config)
        daq_job._put_message_out = MagicMock()

        daq_job._poll_channel_params(mock_device, slots)

        # get_ch_param_info should only be called once for channel 1
        self.assertEqual(mock_device.get_ch_param_info.call_count, 1)
        mock_device.get_ch_param_info.assert_called_with(0, 1)

    def test_handle_set_ch_param_device_not_connected(self):
        """Test handling set param message when device is not connected."""
        daq_job = DAQJobCAENHV(self.config)
        daq_job._device = None

        msg = DAQJobMessageCAENHVSetChParam(
            slot=0,
            channel_list=[0],
            param_name="V0Set",
            value=500.0,
        )

        result = daq_job._handle_set_ch_param(msg)
        self.assertTrue(result)  # Still returns True (message handled)

    def test_handle_set_ch_param_success(self):
        """Test successful handling of set param message."""
        daq_job = DAQJobCAENHV(self.config)
        daq_job._device = MagicMock()

        msg = DAQJobMessageCAENHVSetChParam(
            slot=0,
            channel_list=[0, 1],
            param_name="V0Set",
            value=500.0,
        )

        result = daq_job._handle_set_ch_param(msg)

        self.assertTrue(result)
        daq_job._device.set_ch_param.assert_called_once_with(0, [0, 1], "V0Set", 500.0)

    def test_handle_set_ch_param_error(self):
        """Test handling of set param message when device raises error."""
        daq_job = DAQJobCAENHV(self.config)
        daq_job._device = MagicMock()
        daq_job._device.set_ch_param.side_effect = Exception("Device error")

        msg = DAQJobMessageCAENHVSetChParam(
            slot=0,
            channel_list=[0],
            param_name="V0Set",
            value=500.0,
        )

        result = daq_job._handle_set_ch_param(msg)
        self.assertTrue(result)  # Still returns True (message handled, error logged)

    def test_handle_message_routes_to_set_ch_param(self):
        """Test that handle_message correctly routes to _handle_set_ch_param."""
        daq_job = DAQJobCAENHV(self.config)
        daq_job._device = MagicMock()
        daq_job._handle_set_ch_param = MagicMock(return_value=True)

        msg = DAQJobMessageCAENHVSetChParam(
            slot=0,
            channel_list=[0],
            param_name="V0Set",
            value=500.0,
        )

        result = daq_job.handle_message(msg)

        self.assertTrue(result)
        daq_job._handle_set_ch_param.assert_called_once_with(msg)

    @patch("enrgdaq.daq.jobs.caen.hv.hv")
    def test_poll_skips_none_boards(self, mock_hv):
        """Test that None boards in crate map are skipped."""
        mock_hv.ParamMode.WRONLY = "WRONLY"

        mock_device = MagicMock()
        mock_device.get_ch_param_info.return_value = ["VMon"]
        mock_device.get_ch_param_prop.return_value = MockParamProp(
            mode="RDONLY", param_type="FLOAT"
        )
        mock_device.get_ch_param.return_value = [100.0]

        # Mix of None and real boards (typical crate map)
        slots = [None, MockBoard(1, 2), None, None]

        daq_job = DAQJobCAENHV(self.config)
        daq_job._put_message_out = MagicMock()

        daq_job._poll_channel_params(mock_device, slots)

        # Should only poll board at slot 1, not None slots
        calls = mock_device.get_ch_param_info.call_args_list
        for call in calls:
            self.assertEqual(call[0][0], 1)  # All calls should be for slot 1

    @patch("enrgdaq.daq.jobs.caen.hv.hv")
    @patch(
        "enrgdaq.daq.jobs.caen.hv.get_now_unix_timestamp_ms", return_value=1234567890
    )
    def test_poll_timeseries_config_only(self, mock_timestamp, mock_hv):
        """Test polling with only timeseries_store_config (no store_config)."""
        mock_hv.ParamMode.WRONLY = "WRONLY"

        timeseries_config = MagicMock(spec=DAQJobStoreConfig)
        config = DAQJobCAENHVConfig(
            daq_job_type="DAQJobCAENHV",
            system_type="SY4527",
            link_type="TCPIP",
            connection_arg="192.168.1.100",
            store_config=None,  # No bulk store
            timeseries_store_config=timeseries_config,
        )

        mock_device = MagicMock()
        mock_device.get_ch_param_info.return_value = ["VMon", "IMon"]
        mock_device.get_ch_param_prop.return_value = MockParamProp(
            mode="RDONLY", param_type="FLOAT"
        )
        mock_device.get_ch_param.return_value = [100.5]

        slots = [MockBoard(1, 2)]

        daq_job = DAQJobCAENHV(config)
        daq_job._put_message_out = MagicMock()

        daq_job._poll_channel_params(mock_device, slots)

        # Should send messages for each channel
        # 2 channels, so 2 timeseries messages
        self.assertEqual(daq_job._put_message_out.call_count, 2)

        # Check the first message
        first_call = daq_job._put_message_out.call_args_list[0]
        message = first_call[0][0]
        self.assertIsInstance(message, DAQJobMessageStore)
        self.assertEqual(message.tag, "slot_1_channel_0")
        self.assertIn("timestamp", message.keys)
        self.assertIn("IMon", message.keys)
        self.assertIn("VMon", message.keys)

    @patch("enrgdaq.daq.jobs.caen.hv.hv")
    @patch(
        "enrgdaq.daq.jobs.caen.hv.get_now_unix_timestamp_ms", return_value=1234567890
    )
    def test_poll_both_configs(self, mock_timestamp, mock_hv):
        """Test polling with both store_config and timeseries_store_config."""
        mock_hv.ParamMode.WRONLY = "WRONLY"

        store_config = MagicMock(spec=DAQJobStoreConfig)
        timeseries_config = MagicMock(spec=DAQJobStoreConfig)
        config = DAQJobCAENHVConfig(
            daq_job_type="DAQJobCAENHV",
            system_type="SY4527",
            link_type="TCPIP",
            connection_arg="192.168.1.100",
            store_config=store_config,
            timeseries_store_config=timeseries_config,
        )

        mock_device = MagicMock()
        mock_device.get_ch_param_info.return_value = ["VMon"]
        mock_device.get_ch_param_prop.return_value = MockParamProp(
            mode="RDONLY", param_type="FLOAT"
        )
        mock_device.get_ch_param.return_value = [100.0]

        slots = [MockBoard(0, 1)]

        daq_job = DAQJobCAENHV(config)
        daq_job._put_message_out = MagicMock()

        daq_job._poll_channel_params(mock_device, slots)

        # Should send 2 messages: 1 bulk + 1 timeseries
        self.assertEqual(daq_job._put_message_out.call_count, 2)

        # Find the bulk message (tag = "ch_param")
        bulk_message = None
        timeseries_message = None
        for call in daq_job._put_message_out.call_args_list:
            msg = call[0][0]
            if msg.tag == "ch_param":
                bulk_message = msg
            elif msg.tag.startswith("slot_"):
                timeseries_message = msg

        self.assertIsNotNone(bulk_message)
        self.assertIsNotNone(timeseries_message)
        self.assertEqual(bulk_message.store_config, store_config)
        self.assertEqual(timeseries_message.store_config, timeseries_config)
        self.assertEqual(timeseries_message.tag, "slot_0_channel_0")

    @patch("enrgdaq.daq.jobs.caen.hv.hv")
    @patch(
        "enrgdaq.daq.jobs.caen.hv.get_now_unix_timestamp_ms", return_value=1234567890
    )
    def test_timeseries_tag_format(self, mock_timestamp, mock_hv):
        """Test that timeseries tags follow the slot_X_channel_Y format."""
        mock_hv.ParamMode.WRONLY = "WRONLY"

        timeseries_config = MagicMock(spec=DAQJobStoreConfig)
        config = DAQJobCAENHVConfig(
            daq_job_type="DAQJobCAENHV",
            system_type="SY4527",
            link_type="TCPIP",
            connection_arg="192.168.1.100",
            timeseries_store_config=timeseries_config,
        )

        mock_device = MagicMock()
        mock_device.get_ch_param_info.return_value = ["VMon"]
        mock_device.get_ch_param_prop.return_value = MockParamProp(
            mode="RDONLY", param_type="FLOAT"
        )
        mock_device.get_ch_param.return_value = [100.0]

        # Multiple slots and channels
        slots = [MockBoard(1, 2), MockBoard(3, 1)]

        daq_job = DAQJobCAENHV(config)
        daq_job._put_message_out = MagicMock()

        daq_job._poll_channel_params(mock_device, slots)

        # Should have 3 messages: slot_1_channel_0, slot_1_channel_1, slot_3_channel_0
        self.assertEqual(daq_job._put_message_out.call_count, 3)

        tags = [call[0][0].tag for call in daq_job._put_message_out.call_args_list]
        self.assertIn("slot_1_channel_0", tags)
        self.assertIn("slot_1_channel_1", tags)
        self.assertIn("slot_3_channel_0", tags)


class TestDAQJobCAENHVIntegration(unittest.TestCase):
    """Integration tests for DAQJobCAENHV."""

    def test_message_type_in_allowed_types(self):
        """Test that the message type is in allowed_message_in_types."""
        self.assertIn(
            DAQJobMessageCAENHVSetChParam, DAQJobCAENHV.allowed_message_in_types
        )

    def test_config_type_set(self):
        """Test that config_type is correctly set."""
        self.assertEqual(DAQJobCAENHV.config_type, DAQJobCAENHVConfig)


if __name__ == "__main__":
    unittest.main()
