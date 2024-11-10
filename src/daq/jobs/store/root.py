import os
from typing import Any, cast

import uproot

from daq.models import DAQJobConfig
from daq.store.base import DAQJobStore
from daq.store.models import DAQJobMessageStore, DAQJobStoreConfigROOT
from utils.file import modify_file_path


class DAQJobStoreROOTConfig(DAQJobConfig):
    pass


class DAQJobStoreROOT(DAQJobStore):
    config_type = DAQJobStoreROOTConfig
    allowed_store_config_types = [DAQJobStoreConfigROOT]
    allowed_message_in_types = [DAQJobMessageStore]
    _open_files: dict[str, Any]

    def __init__(self, config: Any, **kwargs):
        super().__init__(config, **kwargs)

        self._open_files = {}

    def handle_message(self, message: DAQJobMessageStore) -> bool:
        super().handle_message(message)
        store_config = cast(DAQJobStoreConfigROOT, message.store_config)
        file_path = modify_file_path(
            store_config.file_path, store_config.add_date, message.prefix
        )

        if file_path not in self._open_files:
            file_exists = os.path.exists(file_path)
            # Create the file if it doesn't exist
            if not file_exists:
                # If file was newly created, do not commit it, close it and
                # switch to update mode on the next iteration
                root_file = uproot.recreate(file_path)
            else:
                root_file = uproot.update(file_path)
                self._open_files[file_path] = root_file
        else:
            file_exists = True
            root_file = self._open_files[file_path]

        data_to_write = {}
        for idx, key in enumerate(message.keys):
            for data in message.data:
                if key not in data_to_write:
                    data_to_write[key] = []
                data_to_write[key].append(data[idx])

        # TODO: This creates a new tree every time we commit. We should probably create tree
        # once and only once, preferably when everything we needed to save is available
        # This kind of depends on the task so it will have to wait
        root_file["tree"] = {key: data_to_write[key] for key in message.keys}
        root_file.file.sink.flush()

        # Close the file if it was newly created
        if not file_exists:
            root_file.file.sink.close()

        return True

    def __del__(self):
        # Close all open files
        for root_file in self._open_files.values():
            if root_file.closed:
                continue
            root_file.file.close()

        return super().__del__()
