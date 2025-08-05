import unittest
from collections import deque
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from enrgdaq.daq.jobs.store.redis import (
    DAQJobStoreRedis,
    DAQJobStoreRedisConfig,
    RedisWriteQueueItem,
)
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    DAQJobStoreConfigRedis,
)


class TestDAQJobStoreRedis(unittest.TestCase):
    def setUp(self):
        self.config = DAQJobStoreRedisConfig(
            daq_job_type="",
            host="localhost",
            port=6379,
            db=0,
            password=None,
        )
        self.store = DAQJobStoreRedis(self.config)

    @patch("time.sleep", return_value=None, side_effect=StopIteration)
    @patch("redis.Redis")
    def test_start(self, mock_redis, mock_sleep):
        with self.assertRaises(StopIteration):
            self.store.start()
        mock_redis.assert_called_once_with(
            host="localhost",
            port=6379,
            db=0,
            password=None,
        )
        self.assertIsNotNone(self.store._connection)

    def test_handle_message(self):
        message = MagicMock(spec=DAQJobMessageStoreTabular)
        message.store_config = MagicMock(
            redis=DAQJobStoreConfigRedis(key="test_key", key_expiration_days=1)
        )
        message.keys = ["header1", "header2"]
        message.data = [["row1_col1", "row1_col2"], ["row2_col1", "row2_col2"]]

        result = self.store.handle_message(message)

        self.assertTrue(result)
        self.assertEqual(len(self.store._write_queue), 2)
        self.assertEqual(self.store._write_queue[0].store_config.key, "test_key")
        self.assertEqual(
            self.store._write_queue[0].data["header1"], ["row1_col1", "row2_col1"]
        )
        self.assertEqual(
            self.store._write_queue[0].data["header2"], ["row1_col2", "row2_col2"]
        )

    def test_store_loop(self):
        self.store._connection = MagicMock()
        self.store._connection.exists = MagicMock(return_value=False)
        self.store._connection.rpush = MagicMock()
        self.store._connection.expire = MagicMock()
        self.store._ts = MagicMock()

        unix_ms = int(datetime.now().timestamp() * 1000)

        self.store._write_queue = deque(
            [
                RedisWriteQueueItem(
                    store_config=DAQJobStoreConfigRedis(
                        key="test_key", key_expiration_days=1
                    ),
                    data={
                        "header1": ["row1_col1", "row2_col1"],
                        "header2": ["row1_col2", "row2_col2"],
                    },
                    tag=None,
                ),
                RedisWriteQueueItem(
                    store_config=DAQJobStoreConfigRedis(
                        key="test_key_no_expiration", key_expiration_days=None
                    ),
                    data={
                        "header1": ["row1_col1", "row2_col1"],
                        "header2": ["row1_col2", "row2_col2"],
                    },
                    tag="tag",
                ),
                RedisWriteQueueItem(
                    store_config=DAQJobStoreConfigRedis(
                        key="test_key_timeseries",
                        key_expiration_days=1,
                        use_timeseries=True,
                    ),
                    data={
                        "timestamp": [unix_ms, unix_ms + 1],
                        "header1": ["row1_col1", "row2_col1"],
                        "header2": ["row1_col2", "row2_col2"],
                    },
                    tag="tag",
                ),
            ]
        )

        date = datetime.now().strftime("%Y-%m-%d")

        self.store.store_loop()

        self.store._connection.rpush.assert_any_call(
            "test_key.header1:" + date, "row1_col1", "row2_col1"
        )
        self.store._connection.rpush.assert_any_call(
            "test_key.header2:" + date, "row1_col2", "row2_col2"
        )

        self.store._connection.rpush.assert_any_call(
            "test_key_no_expiration.tag.header1", "row1_col1", "row2_col1"
        )
        self.store._connection.rpush.assert_any_call(
            "test_key_no_expiration.tag.header2", "row1_col2", "row2_col2"
        )

        self.store._ts.madd.assert_any_call(
            [
                ("test_key_timeseries.tag.timestamp", unix_ms, unix_ms),
                ("test_key_timeseries.tag.timestamp", unix_ms + 1, unix_ms + 1),
                ("test_key_timeseries.tag.header1", unix_ms, "row1_col1"),
                ("test_key_timeseries.tag.header1", unix_ms + 1, "row2_col1"),
                ("test_key_timeseries.tag.header2", unix_ms, "row1_col2"),
                ("test_key_timeseries.tag.header2", unix_ms + 1, "row2_col2"),
            ],
        )

        self.store._ts.create.assert_any_call(
            "test_key_timeseries.tag.header2",
            retention_msecs=int(timedelta(days=1).total_seconds() * 1000),
            labels={"key": "test_key_timeseries", "tag": "tag"},
        )

        self.store._connection.expire.assert_any_call(
            "test_key.header1:" + date, timedelta(days=1)
        )
        self.store._connection.expire.assert_any_call(
            "test_key.header2:" + date, timedelta(days=1)
        )
        self.assertEqual(len(self.store._write_queue), 0)

    def test_handle_message_no_expiration(self):
        message = MagicMock(spec=DAQJobMessageStoreTabular)
        message.store_config = MagicMock(
            redis=DAQJobStoreConfigRedis(key="test_key", key_expiration_days=None)
        )
        message.keys = ["header1", "header2"]
        message.data = [["row1_col1", "row1_col2"], ["row2_col1", "row2_col2"]]

        result = self.store.handle_message(message)

        self.assertTrue(result)
        self.assertEqual(len(self.store._write_queue), 2)
        self.assertEqual(self.store._write_queue[0].store_config.key, "test_key")
        self.assertEqual(
            self.store._write_queue[0].data["header1"], ["row1_col1", "row2_col1"]
        )
        self.assertEqual(
            self.store._write_queue[0].data["header2"], ["row1_col2", "row2_col2"]
        )
        self.assertIsNone(self.store._write_queue[0].store_config.key_expiration_days)

    def test_handle_message_empty_data(self):
        message = MagicMock(spec=DAQJobMessageStoreTabular)
        message.store_config = MagicMock(
            redis=DAQJobStoreConfigRedis(key="test_key", key_expiration_days=1)
        )
        message.keys = ["header1", "header2"]
        message.data = []

        result = self.store.handle_message(message)

        self.assertTrue(result)
        self.assertEqual(len(self.store._write_queue), 0)

    def test_del(self):
        mock_close = MagicMock()
        self.store._connection = MagicMock()
        self.store._connection.close = mock_close  # type: ignore

        del self.store

        mock_close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
