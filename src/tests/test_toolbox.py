import unittest
from unittest.mock import MagicMock, mock_open, patch

from enrgdaq.daq.jobs.caen.toolbox import (
    DAQJobCAENToolbox,
    DAQJobCAENToolboxConfig,
    RegisterLabel,
)


class TestDAQJobCAENToolbox(unittest.TestCase):
    @patch("enrgdaq.daq.jobs.caen.toolbox.which", return_value="/usr/bin/caen-toolbox")
    def setUp(self, mock_which):
        self.config = DAQJobCAENToolboxConfig(
            daq_job_type="",
            digitizer_type="dig1",
            connection_string="-c OPTICAL_LINK -l 0",
            store_config=MagicMock(),
            register_labels=[
                RegisterLabel(register="reg1", label="reg1"),
                RegisterLabel(register="reg2", label="reg2"),
            ],
        )
        self.daq_job = DAQJobCAENToolbox(self.config)

    @patch("enrgdaq.daq.jobs.caen.toolbox.which", return_value="/usr/bin/caen-toolbox")
    def test_init_cli_present(self, mock_which):
        try:
            DAQJobCAENToolbox(self.config)
        except Exception:
            self.fail("DAQJobCAENToolbox raised Exception unexpectedly!")

    @patch("enrgdaq.daq.jobs.caen.toolbox.which", return_value=None)
    def test_init_cli_not_present(self, mock_which):
        with self.assertRaises(Exception) as context:
            DAQJobCAENToolbox(self.config)
        self.assertEqual(str(context.exception), "caen-toolbox cli not found")

    @patch("enrgdaq.daq.jobs.caen.toolbox.subprocess.run")
    @patch("enrgdaq.daq.jobs.caen.toolbox.os.path.exists", return_value=True)
    @patch(
        "enrgdaq.daq.jobs.caen.toolbox.open",
        new_callable=mock_open,
        read_data="reg1,0x1\nreg2,0x2\nwill_not_show_up,0x3\n",
    )
    def test_dump_digitizer(self, mock_open, mock_exists, mock_run):
        mock_run.return_value.returncode = 0
        registers = self.daq_job._dump_digitizer()
        self.assertEqual(registers, {"reg1": 1, "reg2": 2})

    @patch("enrgdaq.daq.jobs.caen.toolbox.subprocess.run")
    @patch("enrgdaq.daq.jobs.caen.toolbox.os.path.exists", return_value=False)
    def test_dump_digitizer_file_not_found(self, mock_exists, mock_run):
        mock_run.return_value.returncode = 0
        with self.assertRaises(Exception) as context:
            self.daq_job._dump_digitizer()
        self.assertEqual(str(context.exception), "Register dump file not found")

    @patch("enrgdaq.daq.jobs.caen.toolbox.subprocess.run")
    def test_dump_digitizer_subprocess_error(self, mock_run):
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = "error"
        with self.assertRaises(Exception) as context:
            self.daq_job._dump_digitizer()
        self.assertEqual(str(context.exception), "caen-toolbox dump failed: error")

    @patch(
        "enrgdaq.daq.jobs.caen.toolbox.DAQJobCAENToolbox._dump_digitizer",
        return_value={"reg1": 1, "reg2": 2},
    )
    @patch(
        "enrgdaq.daq.jobs.caen.toolbox.get_now_unix_timestamp_ms",
        return_value=1234567890,
    )
    @patch("time.sleep", return_value=None)
    @patch("enrgdaq.daq.jobs.caen.toolbox.sleep_for", side_effect=StopIteration)
    @patch("enrgdaq.daq.jobs.caen.toolbox.get_now_unix_timestamp_ms")
    @patch("uuid.uuid4", return_value="testuuid")
    def test_start(
        self,
        mock_uuid,
        mock_get_now_unix_timestamp_ms,
        mock_sleep_for,
        mock_get_now,
        mock_dump_digitizer,
        mock_sleep,
    ):
        mock_get_now_unix_timestamp_ms.return_value = 1234567890
        self.daq_job._put_message_out = MagicMock()
        with self.assertRaises(StopIteration):
            self.daq_job.start()
        message = self.daq_job._put_message_out.call_args[0][0]
        self.assertEqual(message.store_config, self.config.store_config)
        self.assertEqual(message.tag, "caen-toolbox")
        self.assertEqual(message.keys, ["timestamp", "reg1", "reg2"])
        self.assertEqual(message.data, [[1234567890, 1, 2]])


if __name__ == "__main__":
    unittest.main()
