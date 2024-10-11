import csv
import os
from dataclasses import dataclass
from io import TextIOWrapper
from pathlib import Path
from typing import Any, cast

from daq.models import DAQJobConfig
from daq.store.base import DAQJobStore
from daq.store.models import DAQJobMessageStore, DAQJobStoreConfig
from utils.file import add_date_to_file_name


@dataclass
class DAQJobStoreConfigCSV(DAQJobStoreConfig):
    file_path: str
    add_date: bool


@dataclass
class DAQJobStoreCSVConfig(DAQJobConfig):
    pass


class DAQJobStoreCSV(DAQJobStore):
    config_type = DAQJobStoreCSVConfig
    allowed_store_config_types = [DAQJobStoreConfigCSV]
    _open_files: dict[str, TextIOWrapper]

    def __init__(self, config: Any):
        super().__init__(config)
        self._open_files = {}

    def handle_message(self, message: DAQJobMessageStore) -> bool:
        super().handle_message(message)
        store_config = cast(DAQJobStoreConfigCSV, message.store_config)
        file_path = add_date_to_file_name(store_config.file_path, store_config.add_date)

        self._logger.debug(
            f"Handling message for DAQ Job: {type(message.daq_job).__name__}"
        )

        if file_path not in self._open_files:
            file_exists = os.path.exists(file_path)
            # Create the file if it doesn't exist
            if not file_exists:
                Path(file_path).touch()

            # Open file and write csv headers
            file = open(file_path, "a")
            self._open_files[file_path] = file
            writer = csv.writer(file)

            # Write headers if file haven't existed before
            if not file_exists:
                writer.writerow(message.keys)
        else:
            file = self._open_files[file_path]
            writer = csv.writer(file)

        # Write rows and flush
        writer.writerows(message.data)
        file.flush()

        return True

    def __del__(self):
        # Close all open files
        for file in self._open_files.values():
            if file.closed:
                continue
            file.close()

        return super().__del__()
