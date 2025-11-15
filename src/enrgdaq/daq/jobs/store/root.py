import os
from typing import Any, cast

import numpy as np
import uproot

from enrgdaq.daq.models import DAQJobConfig
from enrgdaq.daq.store.base import DAQJobStore
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    DAQJobStoreConfigROOT,
)
from enrgdaq.utils.file import modify_file_path


class DAQJobStoreROOTConfig(DAQJobConfig):
    pass


class DAQJobStoreROOT(DAQJobStore):
    config_type = DAQJobStoreROOTConfig
    allowed_store_config_types = [DAQJobStoreConfigROOT]
    allowed_message_in_types = [DAQJobMessageStoreTabular]
    _open_files: dict[str, Any]

    def __init__(self, config: Any, **kwargs):
        super().__init__(config, **kwargs)

        self._open_files = {}

    def store_loop(self):
        pass

    def handle_message(self, message: DAQJobMessageStoreTabular) -> bool:
        super().handle_message(message)

        print(self.message_in.qsize())

        if message.data_columns is None:
            # Per instructions, only process data_columns for performance.
            return True

        store_config = cast(DAQJobStoreConfigROOT, message.store_config.root)
        file_path = modify_file_path(
            store_config.file_path, store_config.add_date, message.tag
        )

        if file_path not in self._open_files:
            # Ensure directory exists to prevent errors
            dir_name = os.path.dirname(file_path)
            if dir_name:
                os.makedirs(dir_name, exist_ok=True)

            if not os.path.exists(file_path):
                root_file = uproot.recreate(file_path)
            else:
                root_file = uproot.update(file_path)
            self._open_files[file_path] = root_file
        else:
            root_file = self._open_files[file_path]

        # For performance, we assume a fixed tree name. This could be made configurable.
        tree_name = "tree"

        data_to_write = message.data_columns

        rntuple = root_file.mkrntuple(
            tree_name,
            data_to_write,
        )
        rntuple.extend(data_to_write)

        return True

    def __del__(self):
        # Ensure all files are properly closed on exit.
        for root_file in self._open_files.values():
            if not root_file.closed:
                root_file.close()
        self._open_files.clear()

        super().__del__()
