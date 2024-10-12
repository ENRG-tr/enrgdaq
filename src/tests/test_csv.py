import unittest
from datetime import datetime, timedelta
from queue import Queue
from unittest.mock import MagicMock, mock_open, patch

from daq.store.csv import (
    DAQ_JOB_STORE_CSV_FLUSH_INTERVAL_SECONDS,
    CSVFile,
    DAQJobStoreConfigCSV,
    DAQJobStoreCSV,
)
from daq.store.models import DAQJobMessageStore


class TestDAQJobStoreCSV(unittest.TestCase):
    def setUp(self):
        self.config = MagicMock()
        self.store = DAQJobStoreCSV(self.config)

    @patch("daq.store.csv.modify_file_path", return_value="test.csv")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists", return_value=False)
    @patch("pathlib.Path.touch")
    def test_handle_message_new_file(
        self, mock_touch, mock_exists, mock_open, mock_add_date
    ):
        message = MagicMock(spec=DAQJobMessageStore)
        message.store_config = DAQJobStoreConfigCSV(
            daq_job_store_type="csv", file_path="test.csv", add_date=True
        )
        message.keys = ["header1", "header2"]
        message.data = [["row1_col1", "row1_col2"], ["row2_col1", "row2_col2"]]
        message.prefix = None

        self.store.handle_message(message)

        mock_add_date.assert_called_once_with("test.csv", True, None)
        mock_open.assert_called_once_with("test.csv", "a")
        self.assertIn("test.csv", self.store._open_csv_files)
        file = self.store._open_csv_files["test.csv"]
        self.assertEqual(file.write_queue.qsize(), 3)  # 1 header + 2 rows

    @patch("daq.store.csv.modify_file_path", return_value="test.csv")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists", return_value=True)
    def test_handle_message_existing_file(self, mock_exists, mock_open, mock_add_date):
        message = MagicMock(spec=DAQJobMessageStore)
        message.store_config = DAQJobStoreConfigCSV(
            daq_job_store_type="csv", file_path="test.csv", add_date=True
        )
        message.keys = ["header1", "header2"]
        message.data = [["row1_col1", "row1_col2"], ["row2_col1", "row2_col2"]]
        message.prefix = None

        self.store.handle_message(message)

        mock_add_date.assert_called_once_with("test.csv", True, None)
        mock_open.assert_called_once_with("test.csv", "a")
        self.assertIn("test.csv", self.store._open_csv_files)
        file = self.store._open_csv_files["test.csv"]
        self.assertEqual(file.write_queue.qsize(), 2)  # 2 rows only, no header

    def test_flush(self):
        file = CSVFile(
            file=MagicMock(),
            last_flush_date=datetime.now()
            - timedelta(seconds=DAQ_JOB_STORE_CSV_FLUSH_INTERVAL_SECONDS + 1),
            write_queue=Queue(),
        )
        result = self.store._flush(file)
        self.assertTrue(result)
        file.file.flush.assert_called_once()
        self.assertAlmostEqual(
            file.last_flush_date, datetime.now(), delta=timedelta(seconds=1)
        )

    @patch("csv.writer")
    def test_store_loop(self, mock_csv_writer):
        file = CSVFile(
            file=MagicMock(closed=False),
            last_flush_date=datetime.now()
            - timedelta(seconds=DAQ_JOB_STORE_CSV_FLUSH_INTERVAL_SECONDS + 1),
            write_queue=Queue(),
        )
        file.write_queue.put(["row1_col1", "row1_col2"])
        file.write_queue.put(["row2_col1", "row2_col2"])
        self.store._open_csv_files["test.csv"] = file

        mock_writer_instance = mock_csv_writer.return_value

        self.store.store_loop()

        mock_writer_instance.writerows.assert_called()
        self.assertTrue(file.file.flush.called)

    def test_del(self):
        file = CSVFile(
            file=MagicMock(closed=False),
            last_flush_date=datetime.now(),
            write_queue=Queue(),
        )
        self.store._open_csv_files["test.csv"] = file

        del self.store

        file.file.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
