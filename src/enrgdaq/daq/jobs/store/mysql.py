import re
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, cast

import pymysql

from enrgdaq.daq.models import DAQJobConfig
from enrgdaq.daq.store.base import DAQJobStore
from enrgdaq.daq.store.models import DAQJobMessageStore, DAQJobStoreConfigMySQL

DAQ_JOB_STORE_MYSQL_FLUSH_INTERVAL_SECONDS = 15


class DAQJobStoreMySQLConfig(DAQJobConfig):
    host: str
    user: str
    password: str
    database: str
    port: int = 3306


@dataclass
class MySQLWriteQueueItem:
    table_name: str
    keys: list[str]
    rows: list[Any]


class DAQJobStoreMySQL(DAQJobStore):
    config_type = DAQJobStoreMySQLConfig
    allowed_store_config_types = [DAQJobStoreConfigMySQL]
    allowed_message_in_types = [DAQJobMessageStore]

    _write_queue: deque[MySQLWriteQueueItem]
    _last_flush_date: datetime
    _connection: Optional[pymysql.connections.Connection]

    def __init__(self, config: DAQJobStoreMySQLConfig, **kwargs):
        super().__init__(config, **kwargs)

        self._write_queue = deque()
        self._last_flush_date = datetime.now()
        self._connection = None

    def start(self):
        self._connection = pymysql.connect(
            host=self.config.host,
            user=self.config.user,
            port=self.config.port,
            password=self.config.password,
            database=self.config.database,
        )
        super().start()

    def handle_message(self, message: DAQJobMessageStore) -> bool:
        if not super().handle_message(message):
            return False

        store_config = cast(DAQJobStoreConfigMySQL, message.store_config.mysql)

        # Append rows to write_queue
        for row in message.data:
            self._write_queue.append(
                MySQLWriteQueueItem(store_config.table_name, message.keys, row)
            )

        return True

    def _flush(self, force=False):
        assert self._connection is not None
        if (
            datetime.now() - self._last_flush_date
        ).total_seconds() < DAQ_JOB_STORE_MYSQL_FLUSH_INTERVAL_SECONDS and not force:
            return

        self._connection.commit()
        self._last_flush_date = datetime.now()

    def _sanitize_text(self, text: str) -> str:
        # replace anything but letters, numbers, and underscores with underscores
        return re.sub(r"[^a-zA-Z0-9_]", "_", text)

    def store_loop(self):
        assert self._connection is not None
        with self._connection.cursor() as cursor:
            while self._write_queue:
                item = self._write_queue.popleft()

                table_name = self._sanitize_text(item.table_name)
                keys = ",".join(self._sanitize_text(key) for key in item.keys)
                values = ",".join(["%s"] * len(item.keys))
                query = f"INSERT INTO {table_name} ({keys}) VALUES ({values})"
                cursor.execute(
                    query,
                    tuple(item.rows),
                )
            self._flush()

    def __del__(self):
        if self._connection is not None:
            self._flush(force=True)
            if self._connection.open:
                self._connection.close()

        return super().__del__()
