import unittest
from collections import deque
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from enrgdaq.daq.jobs.store.mysql import (
    DAQ_JOB_STORE_MYSQL_FLUSH_INTERVAL_SECONDS,
    DAQJobStoreMySQL,
    DAQJobStoreMySQLConfig,
    MySQLWriteQueueItem,
)
from enrgdaq.daq.store.models import DAQJobMessageStore, DAQJobStoreConfigMySQL


class TestDAQJobStoreMySQL(unittest.TestCase):
    def setUp(self):
        self.config = DAQJobStoreMySQLConfig(
            daq_job_type="",
            host="localhost",
            user="user",
            password="password",
            database="test_db",
        )
        self.store = DAQJobStoreMySQL(self.config)
        self.store._connection = MagicMock(open=True)

    @patch("time.sleep", return_value=None, side_effect=StopIteration)
    @patch("pymysql.connect")
    def test_start(self, mock_connect, mock_sleep):
        with self.assertRaises(StopIteration):
            self.store.start()
        mock_connect.assert_called_once_with(
            host="localhost",
            user="user",
            port=3306,
            password="password",
            database="test_db",
        )
        self.assertIsNotNone(self.store._connection)

    def test_handle_message(self):
        message = MagicMock(spec=DAQJobMessageStore)
        message.store_config = MagicMock(
            mysql=DAQJobStoreConfigMySQL(table_name="test_table")
        )
        message.keys = ["header1", "header2"]
        message.data = [["row1_col1", "row1_col2"], ["row2_col1", "row2_col2"]]

        result = self.store.handle_message(message)

        self.assertTrue(result)
        self.assertEqual(len(self.store._write_queue), 2)
        self.assertEqual(self.store._write_queue[0].table_name, "test_table")
        self.assertEqual(self.store._write_queue[0].keys, ["header1", "header2"])
        self.assertEqual(self.store._write_queue[0].rows, ["row1_col1", "row1_col2"])

    def test_flush(self):
        mock_commit = MagicMock()
        self.store._connection.commit = mock_commit  # type: ignore
        self.store._last_flush_date = datetime.now() - timedelta(
            seconds=DAQ_JOB_STORE_MYSQL_FLUSH_INTERVAL_SECONDS + 1
        )

        self.store._flush()

        mock_commit.assert_called_once()
        self.assertAlmostEqual(
            self.store._last_flush_date, datetime.now(), delta=timedelta(seconds=1)
        )

    def test_store_loop(self):
        mock_cursor = MagicMock()
        mock_cursor.return_value.__enter__.return_value.execute = MagicMock()
        self.store._connection.cursor = mock_cursor  # type: ignore
        self.store._last_flush_date = datetime.now() - timedelta(days=7)

        self.store._write_queue = deque(
            [
                MySQLWriteQueueItem(
                    "test_table", ["header1", "header2"], ["row1_col1", "row1_col2"]
                ),
                MySQLWriteQueueItem(
                    "test_table", ["header1", "header2"], ["row2_col1", "row2_col2"]
                ),
            ]
        )

        mock_cursor_instance = mock_cursor.return_value.__enter__.return_value

        self.store.store_loop()

        mock_cursor_instance.execute.assert_any_call(
            "INSERT INTO test_table (header1,header2) VALUES (%s,%s)",
            ("row1_col1", "row1_col2"),
        )
        mock_cursor_instance.execute.assert_any_call(
            "INSERT INTO test_table (header1,header2) VALUES (%s,%s)",
            ("row2_col1", "row2_col2"),
        )
        self.assertEqual(len(self.store._write_queue), 0)

    def test_del(self):
        mock_commit = MagicMock()
        self.store._connection.commit = mock_commit  # type: ignore
        mock_close = MagicMock()
        self.store._connection.close = mock_close  # type: ignore

        # Explicitly call __del__ to test cleanup behavior
        self.store.__del__()

        mock_commit.assert_called_once()
        mock_close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
