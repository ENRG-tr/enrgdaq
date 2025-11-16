import os
from typing import Any, cast

import uproot
from numpy import ndarray

from enrgdaq.daq.models import DAQJobConfig
from enrgdaq.daq.store.base import DAQJobStore
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    DAQJobStoreConfigROOT,
)
from enrgdaq.utils.file import modify_file_path


class DAQJobStoreROOTConfig(DAQJobConfig):
    out_dir: str = "out/"


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

        data_to_write = message.data_columns
        if not data_to_write:
            return True

        if message.data_columns is None:
            return True

        store_config = cast(DAQJobStoreConfigROOT, message.store_config.root)
        file_path = modify_file_path(
            store_config.file_path, store_config.add_date, message.tag
        )
        file_path = os.path.join(self.config.out_dir, file_path)

        if file_path not in self._open_files or self._open_files[file_path].closed:
            dir_name = os.path.dirname(file_path)
            if dir_name:
                os.makedirs(dir_name, exist_ok=True)

            mode = "recreate" if not os.path.exists(file_path) else "update"
            root_file = getattr(uproot, mode)(file_path)
            self._open_files[file_path] = root_file
            self._logger.info(f"Opened file {file_path}")
        else:
            root_file = self._open_files[file_path]

        tree_name = store_config.tree_name

        if tree_name not in root_file.keys():
            tree = root_file.mktree(
                tree_name,
                {
                    k: v.dtype
                    for k, v in data_to_write.items()
                    if isinstance(v, ndarray)
                },
            )
        else:
            tree = root_file[tree_name]
            assert isinstance(tree, uproot.WritableTree), "Tree is not a WritableTree"

        tree.extend(data_to_write)

        return True

    def __del__(self):
        # Ensure all files are properly closed on exit.
        for root_file in self._open_files.values():
            if not root_file.closed:
                root_file.close()
        self._open_files.clear()

        super().__del__()
