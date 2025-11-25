import os
import time
from queue import Empty
from typing import Any, cast

import uproot
import uproot.compression
from numpy import ndarray

from enrgdaq.daq.models import DAQJobConfig
from enrgdaq.daq.store.base import DAQJobStore
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    DAQJobStoreConfigROOT,
)
from enrgdaq.utils.file import modify_file_path

ROOT_ZSTD_COMPRESSION_LEVEL = 1
ROOT_COMPRESSION_TYPES = {
    "ZSTD": uproot.compression.ZSTD,
    "LZ4": uproot.compression.LZ4,
    "ZLIB": uproot.compression.ZLIB,
    "LZMA": uproot.compression.LZMA,
}


class DAQJobStoreROOTConfig(DAQJobConfig):
    out_dir: str = "out/"


class DAQJobStoreROOT(DAQJobStore):
    config_type = DAQJobStoreROOTConfig
    allowed_store_config_types = [DAQJobStoreConfigROOT]
    allowed_message_in_types = [DAQJobMessageStoreTabular]
    _open_files: dict[str, uproot.WritableDirectory]
    _open_trees: dict[str, dict[str, uproot.WritableTree]]

    def __init__(self, config: Any, **kwargs):
        super().__init__(config, **kwargs)

        self._open_files = {}
        self._open_trees = {}

    def start(self):
        messages = []
        while True:
            start_time = time.time()
            try:
                message = self.message_in.get_nowait()
                self._logger.debug(
                    f"Took {time.time() - start_time} seconds to get message"
                )
                if not DAQJobStoreROOT.can_handle_message(message):
                    continue
                messages.append(message)
            except Empty:
                continue

            if len(messages) == 0:
                time.sleep(0.001)
                continue
            for message in messages:
                self.handle_message(message)  # type: ignore
            messages.clear()

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

            if os.path.exists(file_path):
                root_file = uproot.update(file_path)
            else:
                root_file = uproot.recreate(
                    file_path,
                    compression=ROOT_COMPRESSION_TYPES[store_config.compression_type](
                        level=store_config.compression_level
                    ),
                )

            self._open_files[file_path] = root_file
            self._logger.debug(f"Opened file {file_path}")
        else:
            root_file = self._open_files[file_path]

        tree_name = store_config.tree_name

        if tree_name not in root_file:
            tree = root_file.mktree(
                tree_name,
                {
                    k: v.dtype
                    for k, v in data_to_write.items()
                    if isinstance(v, ndarray)
                },
            )
            self._logger.debug(f"Created tree {tree_name}")
            if file_path not in self._open_trees:
                self._open_trees[file_path] = {}
            self._open_trees[file_path][tree_name] = tree
        else:
            if (
                file_path in self._open_trees
                and tree_name in self._open_trees[file_path]
            ):
                tree = self._open_trees[file_path][tree_name]
            else:
                tree = root_file[tree_name]
            assert isinstance(tree, uproot.WritableTree), "Tree is not a WritableTree"

        start_time = time.time()
        tree.extend(data_to_write)
        self._logger.debug(
            f"Wrote {len(data_to_write)} rows to tree {tree_name} in {time.time() - start_time} seconds"
        )

        return True

    def __del__(self):
        # Ensure all files are properly closed on exit.
        for root_file in self._open_files.values():
            if not root_file.closed:
                root_file.close()
        self._open_files.clear()

        super().__del__()
