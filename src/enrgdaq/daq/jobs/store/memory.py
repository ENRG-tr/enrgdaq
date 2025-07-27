from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from enrgdaq.daq.models import DAQJobConfig
from enrgdaq.daq.store.base import DAQJobStore
from enrgdaq.daq.store.models import (
    DAQJobMessageStore,
    DAQJobMessageStoreTabular,
    DAQJobStoreConfigMemory,
)


class DAQJobStoreMemoryConfig(DAQJobConfig):
    """
    Configuration options for storing DAQ jobs in memory.
    """

    dispose_after_n_entries: Optional[int] = None
    void_data: Optional[bool] = False


@dataclass
class MemoryWriteQueueItem:
    tag: Optional[str]
    keys: list[str]
    rows: list[Any]
    timestamp: datetime


class DAQJobStoreMemory(DAQJobStore):
    config_type = DAQJobStoreMemoryConfig
    allowed_store_config_types = [DAQJobStoreConfigMemory]
    allowed_message_in_types = [DAQJobMessageStore]

    _write_queue: deque[MemoryWriteQueueItem]

    def __init__(self, config: DAQJobStoreMemoryConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._write_queue = deque()
        self._memory = deque()

    def handle_message(self, message: DAQJobMessageStoreTabular) -> bool:
        if not super().handle_message(message):
            return False

        if self.config.void_data:
            return True
        timestamp = datetime.now()
        for row in message.data:
            self._write_queue.append(
                MemoryWriteQueueItem(
                    tag=message.tag,
                    keys=message.keys,
                    rows=row,
                    timestamp=timestamp,
                )
            )
        return True

    def store_loop(self):
        while self._write_queue:
            item = self._write_queue.popleft()
            self._logger.debug("Adding message to memory: " + str(item))
            self._memory.append(item)
            if (
                self.config.dispose_after_n_entries
                and len(self._memory) > self.config.dispose_after_n_entries
            ):
                self._memory.pop()

    def get_all(self) -> list[MemoryWriteQueueItem]:
        return list(self._write_queue)

    def clear(self):
        self._write_queue.clear()

    def __del__(self):
        self.clear()
        return super().__del__()
