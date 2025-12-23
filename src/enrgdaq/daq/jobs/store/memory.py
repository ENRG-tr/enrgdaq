from datetime import datetime
from typing import Optional

from enrgdaq.daq.models import DAQJobConfig
from enrgdaq.daq.store.base import DAQJobStore
from enrgdaq.daq.store.models import (
    DAQJobMessageStore,
    DAQJobMessageStorePyArrow,
    DAQJobMessageStoreTabular,
    DAQJobStoreConfigMemory,
)


class DAQJobStoreMemoryConfig(DAQJobConfig):
    """
    Configuration options for storing DAQ jobs in memory.
    """

    dispose_after_n_entries: Optional[int] = None
    void_data: Optional[bool] = False


class DAQJobStoreMemory(DAQJobStore):
    config_type = DAQJobStoreMemoryConfig
    allowed_store_config_types = [DAQJobStoreConfigMemory]
    allowed_message_in_types = [DAQJobMessageStore]

    def __init__(self, config: DAQJobStoreMemoryConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._memory = {}

    def handle_message(
        self, message: DAQJobMessageStoreTabular | DAQJobMessageStorePyArrow
    ) -> bool:
        if not super().handle_message(message):
            return False

        if self.config.void_data:
            return True

        timestamp = datetime.now()

        if isinstance(message, DAQJobMessageStorePyArrow):
            table = message.table
            if table is None or table.num_rows == 0:
                return True
            self._memory[message.tag] = {
                "timestamp": timestamp,
                "keys": table.column_names,
                "table": table,
            }
        else:
            self._memory[message.tag] = {
                "timestamp": timestamp,
                "keys": message.keys,
                "rows": message.data,
            }
        return True

    def store_loop(self):
        pass

    def get_all(self):
        return self._memory

    def clear(self):
        self._memory.clear()

    def __del__(self):
        self.clear()
        return super().__del__()
