import unittest
from collections import deque
from datetime import datetime, timedelta
from unittest.mock import MagicMock, mock_open, patch

from enrgdaq.daq.jobs.store.raw import (
    DAQ_JOB_STORE_RAW_FLUSH_INTERVAL_SECONDS,
    DAQJobStoreRaw,
    RawFile,
)
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreRaw,
    DAQJobStoreConfig,
    DAQJobStoreConfigRaw,
)


class TestDAQJobStoreRaw(unittest.TestCase):
    def setUp(self):
        self.config = MagicMock(out_dir="out/")
        self.store = DAQJobStoreRaw(self.config)

    @patch("enrgdaq.daq.jobs.store.raw.modify_file_path", return_value="test.raw")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists", return_value=False)
    @patch("pathlib.Path.touch")
    def test_handle_message_new_file(
        self, mock_touch, mock_exists, mock_open, mock_add_date
    ):
        message = MagicMock(spec=DAQJobMessageStoreRaw)
        message.store_config = DAQJobStoreConfig(
            raw=DAQJobStoreConfigRaw(file_path="test.raw", add_date=True)
        )
        message.data = b"raw_data"
        message.tag = None

        self.store.handle_message(message)

        mock_add_date.assert_called_once_with("test.raw", True, None)
        mock_open.assert_called_once_with("out/test.raw", "wb")
        self.assertIn("out/test.raw", self.store._open_raw_files)
        file = self.store._open_raw_files["out/test.raw"]
        self.assertEqual(len(file.write_queue), 1)  # 1 raw data

    @patch("enrgdaq.daq.jobs.store.raw.modify_file_path", return_value="test.raw")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists", return_value=True)
    def test_handle_message_existing_file(self, mock_exists, mock_open, mock_add_date):
        message = MagicMock(spec=DAQJobMessageStoreRaw)
        message.store_config = DAQJobStoreConfig(
            raw=DAQJobStoreConfigRaw(file_path="test.raw", add_date=True)
        )
        message.data = b"raw_data"
        message.tag = None

        self.store.handle_message(message)

        mock_add_date.assert_called_once_with("test.raw", True, None)
        mock_open.assert_called_once_with("out/test.raw", "wb")
        self.assertIn("out/test.raw", self.store._open_raw_files)
        file = self.store._open_raw_files["out/test.raw"]
        self.assertEqual(len(file.write_queue), 1)  # 1 raw data

    def test_flush(self):
        file = RawFile(
            file=MagicMock(),
            last_flush_date=datetime.now()
            - timedelta(seconds=DAQ_JOB_STORE_RAW_FLUSH_INTERVAL_SECONDS + 1),
            write_queue=deque(),
        )
        result = self.store._flush(file)
        self.assertTrue(result)
        file.file.flush.assert_called_once()
        self.assertAlmostEqual(
            file.last_flush_date, datetime.now(), delta=timedelta(seconds=1)
        )

    def test_store_loop(self):
        file = RawFile(
            file=MagicMock(closed=False),
            last_flush_date=datetime.now()
            - timedelta(seconds=DAQ_JOB_STORE_RAW_FLUSH_INTERVAL_SECONDS + 1),
            write_queue=deque(),
        )
        file.write_queue.append(b"raw_data1")
        file.write_queue.append(b"raw_data2")
        self.store._open_raw_files["test.raw"] = file

        self.store.store_loop()

        file.file.write.assert_any_call(b"raw_data1")
        file.file.write.assert_any_call(b"raw_data2")
        self.assertTrue(file.file.flush.called)

    def test_del(self):
        file = RawFile(
            file=MagicMock(closed=False),
            last_flush_date=datetime.now(),
            write_queue=deque(),
        )
        self.store._open_raw_files["test.raw"] = file

        del self.store

        file.file.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
