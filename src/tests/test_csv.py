import os
import unittest
from collections import deque
from datetime import datetime, timedelta
from unittest.mock import MagicMock, mock_open, patch

from enrgdaq.daq.jobs.store.csv import (
    DAQ_JOB_STORE_CSV_FLUSH_INTERVAL_SECONDS,
    CSVFile,
    DAQJobStoreConfigCSV,
    DAQJobStoreCSV,
)
from enrgdaq.daq.store.models import DAQJobMessageStore, DAQJobStoreConfig
from enrgdaq.utils.file import modify_file_path


class TestDAQJobStoreCSV(unittest.TestCase):
    def setUp(self):
        self.config = MagicMock(out_dir="out/")
        self.store = DAQJobStoreCSV(self.config)

    def test_modify_file_path(self):
        file_path = "test.csv"
        add_date = True
        tag = "tag"
        now = datetime.now()
        test_dates = [
            (datetime(2023, 10, 20), "2023-10-20_test_tag.csv"),
            (datetime(2023, 1, 2), "2023-01-02_test_tag.csv"),
            (None, f"{now.year}-{now.month:02d}-{now.day:02d}_test_tag.csv"),
        ]
        for date, expected_file_path in test_dates:
            expected_file_path = expected_file_path.replace("/", os.path.sep)
            actual_file_path = modify_file_path(file_path, add_date, tag, date)
            self.assertEqual(expected_file_path, actual_file_path)

        # Test with date + folder
        file_path = "boo/test.csv"
        add_date = True
        tag = "tag"
        expected_file_path = (
            f"boo/{now.year}-{now.month:02d}-{now.day:02d}_test_tag.csv"
        )
        actual_file_path = modify_file_path(file_path, add_date, tag)
        self.assertEqual(expected_file_path, actual_file_path)

    @patch("enrgdaq.daq.jobs.store.csv.modify_file_path", return_value="test.csv")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists", return_value=False)
    @patch("pathlib.Path.touch")
    def test_handle_message_new_file(
        self, mock_touch, mock_exists, mock_open, mock_add_date
    ):
        message = MagicMock(spec=DAQJobMessageStore)
        message.store_config = DAQJobStoreConfig(
            csv=DAQJobStoreConfigCSV(file_path="test.csv", add_date=True)
        )
        message.keys = ["header1", "header2"]
        message.data = [["row1_col1", "row1_col2"], ["row2_col1", "row2_col2"]]
        message.tag = None

        self.store.handle_message(message)

        mock_add_date.assert_called_once_with("test.csv", True, None)
        mock_open.assert_called_once_with("out/test.csv", "a", newline="")
        self.assertIn("out/test.csv", self.store._open_csv_files)
        file = self.store._open_csv_files["out/test.csv"]
        self.assertEqual(len(file.write_queue), 3)  # 1 header + 2 rows

    @patch("enrgdaq.daq.jobs.store.csv.modify_file_path", return_value="test.csv")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists", return_value=True)
    def test_handle_message_existing_file(self, mock_exists, mock_open, mock_add_date):
        message = MagicMock(spec=DAQJobMessageStore)
        message.store_config = DAQJobStoreConfig(
            csv=DAQJobStoreConfigCSV(file_path="test.csv", add_date=True)
        )
        message.keys = ["header1", "header2"]
        message.data = [["row1_col1", "row1_col2"], ["row2_col1", "row2_col2"]]
        message.tag = None

        self.store.handle_message(message)

        mock_add_date.assert_called_once_with("test.csv", True, None)
        mock_open.assert_called_once_with("out/test.csv", "a", newline="")
        self.assertIn("out/test.csv", self.store._open_csv_files)
        file = self.store._open_csv_files["out/test.csv"]
        self.assertEqual(len(file.write_queue), 2)  # 2 rows only, no header

    def test_flush(self):
        file = CSVFile(
            file=MagicMock(),
            last_flush_date=datetime.now()
            - timedelta(seconds=DAQ_JOB_STORE_CSV_FLUSH_INTERVAL_SECONDS + 1),
            write_queue=deque(),
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
            write_queue=deque(),
        )
        file.write_queue.append(["row1_col1", "row1_col2"])
        file.write_queue.append(["row2_col1", "row2_col2"])
        self.store._open_csv_files["test.csv"] = file

        mock_writer_instance = mock_csv_writer.return_value

        self.store.store_loop()

        mock_writer_instance.writerows.assert_called()
        self.assertTrue(file.file.flush.called)

    @patch("csv.writer")
    def test_store_loop_writerows(self, mock_csv_writer):
        file = CSVFile(
            file=MagicMock(closed=False),
            last_flush_date=datetime.now()
            - timedelta(seconds=DAQ_JOB_STORE_CSV_FLUSH_INTERVAL_SECONDS + 1),
            write_queue=deque(),
        )
        file.write_queue.append(["row1_col1", "row1_col2"])
        file.write_queue.append(["row2_col1", "row2_col2"])
        self.store._open_csv_files["test.csv"] = file

        mock_writer_instance = mock_csv_writer.return_value

        self.store.store_loop()

        mock_writer_instance.writerows.assert_called_with(
            [["row1_col1", "row1_col2"], ["row2_col1", "row2_col2"]]
        )
        self.assertTrue(file.file.flush.called)

    def test_del(self):
        file = CSVFile(
            file=MagicMock(closed=False),
            last_flush_date=datetime.now(),
            write_queue=deque(),
        )
        self.store._open_csv_files["test.csv"] = file

        del self.store

        file.file.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
