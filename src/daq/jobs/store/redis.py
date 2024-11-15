from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional, cast

import redis
import redis.exceptions

from daq.models import DAQJobConfig
from daq.store.base import DAQJobStore
from daq.store.models import DAQJobMessageStore, DAQJobStoreConfigRedis


class DAQJobStoreRedisConfig(DAQJobConfig):
    host: str
    port: int = 6379
    db: int = 0
    password: Optional[str] = None


@dataclass
class RedisWriteQueueItem:
    redis_key: str
    data: dict[str, list[Any]]
    expiration: Optional[timedelta]
    prefix: Optional[str]


class DAQJobStoreRedis(DAQJobStore):
    config_type = DAQJobStoreRedisConfig
    allowed_store_config_types = [DAQJobStoreConfigRedis]
    allowed_message_in_types = [DAQJobMessageStore]

    _write_queue: deque[RedisWriteQueueItem]
    _last_flush_date: datetime
    _connection: Optional[redis.Redis]
    _check_keys_for_removal: deque[DAQJobStoreConfigRedis] = deque()

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
        super().start()

    def handle_message(self, message: DAQJobMessageStore) -> bool:
        if not super().handle_message(message):
            return False

        store_config = cast(DAQJobStoreConfigRedis, message.store_config.redis)
        key_expiration = None
        if store_config.key_expiration_days is not None:
            key_expiration = timedelta(days=store_config.key_expiration_days)

        data = {}
        # Add data to data dict that we can add to Redis
        for i, row in enumerate(message.keys):
            data[row] = [x[i] for x in message.data]

        # Append rows to write_queue
        for row in message.data:
            self._write_queue.append(
                RedisWriteQueueItem(
                    store_config.key,
                    data,
                    key_expiration,
                    message.prefix,
                )
            )

        # Append keys to check_keys
        self._check_keys_for_removal.append(store_config)

        return True

    def store_loop(self):
        assert self._connection is not None
        while self._write_queue:
            item = self._write_queue.popleft()

            # Append item to key in redis
            for key, values in item.data.items():
                item_key = f"{item.redis_key}.{key}"
                if item.prefix is not None:
                    item_key = f"{item.prefix}.{item_key}"

                # Add date to key if expiration is set
                if item.expiration is not None:
                    item_key += ":" + datetime.now().strftime("%Y-%m-%d")

                item_exists = self._connection.exists(item_key)
                self._connection.rpush(item_key, *values)

                # Set expiration if it was newly created
                if not item_exists and item.expiration is not None:
                    self._connection.expire(item_key, item.expiration)

    def __del__(self):
        try:
            if self._connection is not None:
                self._connection.close()
        except redis.exceptions.ConnectionError:
            pass

        return super().__del__()
