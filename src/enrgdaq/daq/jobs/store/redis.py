import base64
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional, cast

import redis
import redis.exceptions
from redis.commands.timeseries import TimeSeries

from enrgdaq.daq.models import DAQJobConfig
from enrgdaq.daq.store.base import DAQJobStore
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreRaw,
    DAQJobMessageStoreTabular,
    DAQJobStoreConfigRedis,
)


class DAQJobStoreRedisConfig(DAQJobConfig):
    host: str
    port: int = 6379
    db: int = 0
    password: Optional[str] = None


@dataclass
class RedisWriteQueueItem:
    store_config: DAQJobStoreConfigRedis
    data: dict[str, list[Any]] | bytes
    tag: Optional[str]


class DAQJobStoreRedis(DAQJobStore):
    config_type = DAQJobStoreRedisConfig
    allowed_store_config_types = [DAQJobStoreConfigRedis]
    allowed_message_in_types = [DAQJobMessageStoreTabular, DAQJobMessageStoreRaw]

    _write_queue: deque[RedisWriteQueueItem]
    _last_flush_date: datetime
    _connection: Optional[redis.Redis]
    _ts: Optional[TimeSeries]

    def __init__(self, config: DAQJobStoreRedisConfig, **kwargs):
        super().__init__(config, **kwargs)

        self._write_queue = deque()
        self._last_flush_date = datetime.now()
        self._connection = None

    def start(self):
        self._connection = redis.Redis(
            host=self.config.host,
            port=self.config.port,
            db=self.config.db,
            password=self.config.password,
        )
        try:
            self._ts = self._connection.ts()
        except Exception as ex:
            self._logger.error("Timeseries not supported by Redis server", exc_info=ex)
            self._ts = None

        super().start()

    def handle_message(
        self, message: DAQJobMessageStoreTabular | DAQJobMessageStoreRaw
    ) -> bool:
        if not super().handle_message(message):
            return False

        store_config = cast(DAQJobStoreConfigRedis, message.store_config.redis)

        data = {}
        if isinstance(message, DAQJobMessageStoreTabular):
            # Add data to data dict that we can add to Redis
            for i, row in enumerate(message.keys):
                data[row] = [x[i] for x in message.data]
            for row in message.data:
                self._write_queue.append(
                    RedisWriteQueueItem(store_config, data, message.tag)
                )
        else:
            data = message.data
            self._write_queue.append(
                RedisWriteQueueItem(store_config, data, message.tag)
            )

        return True

    def store_loop(self):
        assert self._connection is not None
        while self._write_queue:
            msg = self._write_queue.popleft()
            if msg.store_config.use_timeseries and self._ts is None:
                self._logger.warning(
                    "Trying to store data in timeseries, but timeseries is not supported by Redis server"
                )
                return

            key_expiration = None
            if msg.store_config.key_expiration_days is not None:
                key_expiration = timedelta(days=msg.store_config.key_expiration_days)

            base_item_key = f"{msg.store_config.key}{msg.tag if msg.tag else ''}"
            if isinstance(msg.data, bytes):
                item_key = msg.store_config.key
                data = base64.b64encode(msg.data).decode("utf-8")
                self._connection.set(item_key, data, ex=key_expiration)
                continue
            if not isinstance(msg.data, dict):
                self._logger.error(
                    "msg.data must be a dict or bytes, but got " f"{type(msg.data)}"
                )
                continue

            batched_adds = []
            # Append item to key in redis
            for item in msg.data.items():
                key, values = item
                item_key = f"{base_item_key}.{key}"

                if not msg.store_config.use_timeseries:
                    # Add date to key if expiration is set
                    if key_expiration is not None:
                        item_key += ":" + datetime.now().strftime("%Y-%m-%d")

                    item_exists = self._connection.exists(item_key)
                    self._connection.rpush(item_key, *values)

                    # Set expiration if it was newly created
                    if not item_exists and key_expiration is not None:
                        self._connection.expire(item_key, key_expiration)
                    continue

                # Save it via Redis TimeSeries
                assert self._ts is not None

                # Create TimeSeries key if it doesn't exist
                if not self._connection.exists(item_key) and key != "timestamp":
                    retention_msecs = None
                    if msg.store_config.key_expiration_days is not None:
                        retention_msecs = int(
                            timedelta(
                                days=msg.store_config.key_expiration_days
                            ).total_seconds()
                            * 1000
                        )
                    self._ts.create(
                        item_key,
                        retention_msecs=retention_msecs,
                        labels={"key": msg.store_config.key}
                        | ({"tag": msg.tag} if msg.tag else {}),
                    )
                if "timestamp" not in msg.data:
                    self._logger.warning(
                        "Message data does not contain a timestamp, skipping"
                    )
                    return

                batched_adds.extend(
                    [
                        (item_key, msg.data["timestamp"][i], value)
                        for i, value in enumerate(values)
                    ]
                )

            if self._ts is not None and len(batched_adds) > 0:
                self._ts.madd(batched_adds)

    def __del__(self):
        try:
            if self._connection is not None:
                self._connection.close()
        except redis.exceptions.ConnectionError:
            pass

        return super().__del__()
