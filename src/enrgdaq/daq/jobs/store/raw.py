import os
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, cast

from enrgdaq.daq.models import DAQJobConfig
from enrgdaq.daq.store.base import DAQJobStore
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreRaw,
    DAQJobStoreConfigRaw,
)
from enrgdaq.utils.file import modify_file_path

DAQ_JOB_STORE_RAW_FLUSH_INTERVAL_SECONDS = 1


class DAQJobStoreRawConfig(DAQJobConfig):
    out_dir: str = "out/"


@dataclass
class RawFile:
    file: Any
    last_flush_date: datetime
    write_queue: deque[bytes]
    overwrite: Optional[bool] = None


class DAQJobStoreRaw(DAQJobStore):
    config_type = DAQJobStoreRawConfig
    allowed_store_config_types = [DAQJobStoreConfigRaw]
    allowed_message_in_types = [DAQJobMessageStoreRaw]

    _open_raw_files: dict[str, RawFile]

    def __init__(self, config: DAQJobStoreRawConfig, **kwargs):
        super().__init__(config, **kwargs)

        self._open_raw_files = {}

    def handle_message(self, message: DAQJobMessageStoreRaw) -> bool:
        if not super().handle_message(message):
            return False

        store_config = cast(DAQJobStoreConfigRaw, message.store_config.raw)
        file_path = modify_file_path(
            store_config.file_path, store_config.add_date, message.tag
        )
        file_path = os.path.join(self.config.out_dir, file_path)

        file, new_file = self._open_raw_file(file_path, store_config.overwrite)
        if file.overwrite:
            file.write_queue.clear()

        # Append raw data to write_queue
        file.write_queue.append(message.data)

        return True

    def _open_raw_file(
        self, file_path: str, overwrite: Optional[bool]
    ) -> tuple[RawFile, bool]:
        """
        Opens a file and returns (RawFile, new_file)
        """
        if file_path not in self._open_raw_files:
            file_exists = os.path.exists(file_path)
            # Create the file if it doesn't exist
            if not file_exists:
                # Create the directory if it doesn't exist
                Path(os.path.dirname(file_path)).mkdir(parents=True, exist_ok=True)
                Path(file_path).touch()

            file_handle = open(file_path, "ab" if not overwrite else "wb")

            # Open file
            file = RawFile(
                file_handle,
                datetime.now(),
                deque(),
                overwrite,
            )
            self._open_raw_files[file_path] = file
        else:
            file_exists = True
            file = self._open_raw_files[file_path]
        return file, not file_exists

    def _flush(self, file: RawFile) -> bool:
        if (
            datetime.now() - file.last_flush_date
        ).total_seconds() < DAQ_JOB_STORE_RAW_FLUSH_INTERVAL_SECONDS:
            return False

        self._logger.debug("Flushing raw file")
        file.file.flush()
        file.last_flush_date = datetime.now()
        return True

    def store_loop(self):
        files_to_delete = []
        for file_path, file in self._open_raw_files.items():
            if file.file.closed:
                files_to_delete.append(file_path)
                continue

            while file.write_queue:
                item = file.write_queue.popleft()
                self._logger.debug("Writing raw file " + str(len(item)))
                file.file.write(item)

            if file.overwrite:
                file.file.close()
                files_to_delete.append(file_path)
                continue

            # Flush if the flush time is up
            self._flush(file)

        for file_path in files_to_delete:
            del self._open_raw_files[file_path]

    def __del__(self):
        self.store_loop()

        # Close all open files
        for file in self._open_raw_files.values():
            if file.file.closed:
                continue
            file.file.close()

        return super().__del__()
