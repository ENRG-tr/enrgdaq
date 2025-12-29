import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, cast

import pyarrow as pa
import uproot
import uproot.compression
from numpy import ndarray

from enrgdaq.daq.models import DAQJobConfig
from enrgdaq.daq.store.base import DAQJobStore
from enrgdaq.daq.store.models import (
    DAQJobMessageStorePyArrow,
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

# uproot recommends at least 100 kB per branch per extend() call
BUFFER_SIZE_THRESHOLD_BYTES = 5_000_000
BUFFER_FLUSH_INTERVAL_SECONDS = 1.0


@dataclass
class TreeBuffer:
    """Buffer for accumulating data before writing to ROOT tree."""

    tables: list[pa.Table] = field(default_factory=list)
    total_bytes: int = 0
    last_flush_time: datetime = field(default_factory=datetime.now)
    store_config: DAQJobStoreConfigROOT | None = None


class DAQJobStoreROOTConfig(DAQJobConfig):
    out_dir: str = "out/"
    buffer_size_bytes: int = BUFFER_SIZE_THRESHOLD_BYTES
    flush_interval_seconds: float = BUFFER_FLUSH_INTERVAL_SECONDS


class DAQJobStoreROOT(DAQJobStore):
    """
    ROOT file store with buffered writes for high throughput.

    Buffers incoming data and writes to ROOT in batches to minimize
    TBasket overhead. Per uproot docs, each extend() should be at least
    100 kB per branch for efficient writing.
    """

    config_type = DAQJobStoreROOTConfig
    config: DAQJobStoreROOTConfig
    allowed_store_config_types = [DAQJobStoreConfigROOT]
    allowed_message_in_types = [DAQJobMessageStoreTabular, DAQJobMessageStorePyArrow]

    _open_files: dict[str, uproot.WritableDirectory]
    _open_trees: dict[str, dict[str, uproot.WritableTree]]
    _buffers: dict[tuple[str, str], TreeBuffer]  # (file_path, tree_name) -> buffer

    def __init__(self, config: Any, **kwargs):
        super().__init__(config, **kwargs)

        self._open_files = {}
        self._open_trees = {}
        self._buffers = {}

    def start(self):
        while True:
            messages_processed = self.consume_all()
            self._flush_ready_buffers()

            if not messages_processed:
                time.sleep(0.001)

    def handle_message(
        self, message: DAQJobMessageStoreTabular | DAQJobMessageStorePyArrow
    ) -> bool:
        super().handle_message(message)

        # Convert message to PyArrow table
        if isinstance(message, DAQJobMessageStorePyArrow):
            table = message.get_table()
            message.release()
            if table.num_rows == 0:
                return True
        else:
            # Convert tabular data to PyArrow table
            data_columns = message.data_columns
            if not data_columns:
                self._logger.warning(
                    "Received tabular message with no data_column, DAQJobStoreROOT does not support that."
                )
                return True
            table = pa.table(data_columns)

        store_config = cast(DAQJobStoreConfigROOT, message.store_config.root)
        file_path = modify_file_path(
            store_config.file_path, store_config.add_date, message.tag
        )
        file_path = os.path.join(self.config.out_dir, file_path)
        tree_name = store_config.tree_name

        # Get or create buffer for this file/tree
        buffer_key = (file_path, tree_name)
        if buffer_key not in self._buffers:
            self._buffers[buffer_key] = TreeBuffer(store_config=store_config)

        buffer = self._buffers[buffer_key]

        # Add table to buffer
        buffer.tables.append(table)
        buffer.total_bytes += table.nbytes

        # Check if we should flush immediately (buffer too large)
        if buffer.total_bytes >= self.config.buffer_size_bytes:
            self._flush_buffer(file_path, tree_name)

        return True

    def _flush_ready_buffers(self):
        """Flush buffers that have exceeded the time threshold."""
        now = datetime.now()
        buffers_to_flush = []

        for (file_path, tree_name), buffer in self._buffers.items():
            if not buffer.tables:
                continue

            time_since_flush = (now - buffer.last_flush_time).total_seconds()
            if time_since_flush >= self.config.flush_interval_seconds:
                buffers_to_flush.append((file_path, tree_name))

        for file_path, tree_name in buffers_to_flush:
            self._flush_buffer(file_path, tree_name)

    def _flush_buffer(self, file_path: str, tree_name: str):
        """Flush buffered data to ROOT file."""
        buffer_key = (file_path, tree_name)
        if buffer_key not in self._buffers:
            return

        buffer = self._buffers[buffer_key]
        if not buffer.tables:
            return

        store_config = buffer.store_config
        assert store_config is not None, "Buffer has no store_config"

        # Combine all buffered tables into one
        combined_table = pa.concat_tables(buffer.tables)

        # Convert to dict of numpy arrays for uproot
        data_to_write = {
            col_name: combined_table.column(col_name).to_numpy()
            for col_name in combined_table.column_names
        }

        # Open or get ROOT file
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

        # Create or get tree
        # Check our cache first - if we have the tree cached, use it
        if file_path in self._open_trees and tree_name in self._open_trees[file_path]:
            tree = self._open_trees[file_path][tree_name]
        elif tree_name not in root_file:
            # Tree doesn't exist, create it
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
            # Tree exists in file but not in our cache - this shouldn't happen
            # in normal operation. It can occur if a file was left from a previous
            # run. The caller should clean up output files before starting.
            raise RuntimeError(
                f"Tree '{tree_name}' already exists in file '{file_path}' but was not created "
                f"by this process. Please remove the file and try again."
            )

        # Write the combined data in one extend() call
        start_time = time.time()
        tree.extend(data_to_write)
        write_time = time.time() - start_time

        self._logger.debug(
            f"Flushed {len(buffer.tables)} messages ({buffer.total_bytes / 1_000_000:.2f} MB, "
            f"{combined_table.num_rows} rows) to {tree_name} in {write_time:.3f}s"
        )

        # Clear buffer
        buffer.tables.clear()
        buffer.total_bytes = 0
        buffer.last_flush_time = datetime.now()

    def _flush_all_buffers(self):
        """Flush all buffers regardless of thresholds."""
        for file_path, tree_name in list(self._buffers.keys()):
            self._flush_buffer(file_path, tree_name)

    def __del__(self):
        # Flush any remaining buffered data
        try:
            self._flush_all_buffers()
        except Exception:
            pass

        # Ensure all files are properly closed on exit
        for root_file in self._open_files.values():
            if not root_file.closed:
                root_file.close()
        self._open_files.clear()

        super().__del__()
