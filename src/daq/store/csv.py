import csv
import os
from dataclasses import dataclass
from datetime import datetime
from io import TextIOWrapper
from pathlib import Path
from queue import Empty, Queue
from typing import Any, cast

from daq.models import DAQJobConfig
from daq.store.base import DAQJobStore
from daq.store.models import DAQJobMessageStore, DAQJobStoreConfig
from utils.file import add_date_to_file_name

DAQ_JOB_STORE_CSV_FLUSH_INTERVAL_SECONDS = 5 * 60
DAQ_JOB_STORE_CSV_WRITE_BATCH_SIZE = 1000


@dataclass
class DAQJobStoreConfigCSV(DAQJobStoreConfig):
    file_path: str
    add_date: bool


@dataclass
class DAQJobStoreCSVConfig(DAQJobConfig):
    pass


@dataclass
class CSVFile:
    file: TextIOWrapper
    last_flush_date: datetime
    write_queue: Queue[list[Any]]


class DAQJobStoreCSV(DAQJobStore):
    config_type = DAQJobStoreCSVConfig
    allowed_store_config_types = [DAQJobStoreConfigCSV]
    allowed_message_in_types = [DAQJobMessageStore]

    _open_csv_files: dict[str, CSVFile]

    def __init__(self, config: Any):
        super().__init__(config)
        self._open_csv_files = {}

    def handle_message(self, message: DAQJobMessageStore) -> bool:
        super().handle_message(message)
        store_config = cast(DAQJobStoreConfigCSV, message.store_config)
        file_path = add_date_to_file_name(store_config.file_path, store_config.add_date)
        file, new_file = self._open_csv_file(file_path)

        # Write headers if the file is new
        if new_file:
            file.write_queue.put(message.keys)

        # Append rows to write_queue
        for row in message.data:
            file.write_queue.put(row)

        return True

    def _open_csv_file(self, file_path: str) -> tuple[CSVFile, bool]:
        """
        Opens a file and returns (CSVFile, new_file)
        """
        if file_path not in self._open_csv_files:
            file_exists = os.path.exists(file_path)
            # Create the file if it doesn't exist
            if not file_exists:
                Path(file_path).touch()

            # Open file
            file = CSVFile(open(file_path, "a"), datetime.now(), Queue())
            self._open_csv_files[file_path] = file
        else:
            file_exists = True
            file = self._open_csv_files[file_path]
        return file, not file_exists

    def _flush(self, file: CSVFile) -> bool:
        if (
            datetime.now() - file.last_flush_date
        ).total_seconds() < DAQ_JOB_STORE_CSV_FLUSH_INTERVAL_SECONDS:
            return False

        file.file.flush()
        file.last_flush_date = datetime.now()
        return True

    def store_loop(self):
        for file in self._open_csv_files.values():
            if file.file.closed:
                continue
            writer = csv.writer(file.file)
            total_rows_to_write = 0
            rows_to_write = []

            # Write rows in batches
            while True:
                rows_to_write.clear()
                for _ in range(DAQ_JOB_STORE_CSV_WRITE_BATCH_SIZE):
                    try:
                        rows_to_write.append(file.write_queue.get_nowait())
                    except Empty:
                        break
                if len(rows_to_write) == 0:
                    break
                total_rows_to_write += len(rows_to_write)
                writer.writerows(rows_to_write)

            # Flush if the flush time is up
            if self._flush(file):
                self._logger.debug(
                    f"Flushed {total_rows_to_write} rows to '{file.file.name}'"
                )

    def __del__(self):
        self.store_loop()

        # Close all open files
        for file in self._open_csv_files.values():
            if file.file.closed:
                continue
            file.file.close()

        return super().__del__()
