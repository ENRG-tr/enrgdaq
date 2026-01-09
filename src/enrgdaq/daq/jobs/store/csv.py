import csv
import gzip
import io
import os
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, TextIO, cast

import numpy as np
import pyarrow as pa
import pyarrow.csv as pa_csv
from numpy import ndarray

from enrgdaq.daq.models import DAQJobConfig
from enrgdaq.daq.store.base import DAQJobStore
from enrgdaq.daq.store.models import (
    DAQJobMessageStore,
    DAQJobMessageStorePyArrow,
    DAQJobMessageStoreTabular,
    DAQJobStoreConfigCSV,
)
from enrgdaq.utils.file import modify_file_path

DAQ_JOB_STORE_CSV_FLUSH_INTERVAL_SECONDS = 1


class DAQJobStoreCSVConfig(DAQJobConfig):
    out_dir: str = "out/"


@dataclass
class CSVFile:
    file: TextIO
    last_flush_date: datetime
    write_queue: deque[list[Any]]
    arrow_tables: list[pa.Table] = field(default_factory=list)
    header_written: bool = False
    overwrite: Optional[bool] = None


class DAQJobStoreCSV(DAQJobStore):
    config_type = DAQJobStoreCSVConfig
    allowed_store_config_types = [DAQJobStoreConfigCSV]
    allowed_message_in_types = [DAQJobMessageStore]

    _open_csv_files: dict[str, CSVFile]

    def __init__(self, config: DAQJobStoreCSVConfig, **kwargs):
        super().__init__(config, **kwargs)

        self._open_csv_files = {}

    def handle_message(
        self, message: DAQJobMessageStoreTabular | DAQJobMessageStorePyArrow
    ) -> bool:
        if not super().handle_message(message):
            return False

        store_config = cast(DAQJobStoreConfigCSV, message.store_config.csv)
        file_path = modify_file_path(
            store_config.file_path, store_config.add_date, message.tag
        )
        file_path = os.path.join(self.config.out_dir, file_path)

        file, new_file = self._open_csv_file(
            file_path, store_config.overwrite, store_config.use_gzip
        )
        if file.overwrite:
            file.write_queue.clear()

        # Handle PyArrow messages
        if isinstance(message, DAQJobMessageStorePyArrow):
            table = message.get_table()
            message.release()
            if table.num_rows == 0:
                return True
            file.arrow_tables.append(table)
            return True

        # Handle Tabular messages
        # Write headers if the file is new
        if new_file or file.overwrite:
            file.write_queue.append(message.keys)

        # Append rows to write_queue
        if message.data is not None:
            for row in message.data:
                file.write_queue.append(row)
        elif message.data_columns is not None:
            cols = [message.data_columns[k] for k in message.keys]
            if cols and all(isinstance(c, ndarray) for c in cols):
                data_rows = np.stack(cols, axis=-1)
                file.write_queue.extend(data_rows)
            else:
                for row in zip(*cols):
                    file.write_queue.append(list(row))

        return True

    def _open_csv_file(
        self, file_path: str, overwrite: Optional[bool], use_gzip: Optional[bool]
    ) -> tuple[CSVFile, bool]:
        """
        Opens a file and returns (CSVFile, new_file)
        """
        if file_path not in self._open_csv_files:
            file_exists = os.path.exists(file_path)
            # Check if existing file has content (meaning header was already written)
            file_has_content = file_exists and os.path.getsize(file_path) > 0
            # Create the file if it doesn't exist
            if not file_exists:
                # Create the directory if it doesn't exist
                Path(os.path.dirname(file_path)).mkdir(parents=True, exist_ok=True)
                Path(file_path).touch()

            if use_gzip:
                file_handle = gzip.open(
                    file_path, "at" if not overwrite else "wt", newline=""
                )
            else:
                file_handle = open(file_path, "a" if not overwrite else "w", newline="")

            # Open file
            # If file already has content and we're appending, assume header was written
            file = CSVFile(
                file_handle,
                datetime.now(),
                deque(),
                overwrite=overwrite,
                header_written=file_has_content and not overwrite,
            )
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
        files_to_delete = []
        for file_path, file in self._open_csv_files.copy().items():
            if file.file.closed:
                files_to_delete.append(file_path)
                continue

            # Write PyArrow tables efficiently using native CSV writer
            if file.arrow_tables:
                combined = pa.concat_tables(file.arrow_tables)
                file.arrow_tables.clear()

                # Use PyArrow's native CSV writer (much faster)
                write_options = pa_csv.WriteOptions(
                    include_header=not file.header_written
                )

                # Write to a BytesIO buffer first, then decode and write to text file
                # PyArrow's write_csv requires a binary file, but we have a text file
                buffer = io.BytesIO()
                pa_csv.write_csv(combined, buffer, write_options=write_options)
                file.file.write(buffer.getvalue().decode("utf-8"))
                file.header_written = True

            # Write traditional queue rows
            writer = csv.writer(file.file)
            row_size = len(file.write_queue)
            if row_size > 0:
                if not file.header_written:
                    # First row is header for tabular messages
                    pass
                writer.writerows(list(file.write_queue))
            file.write_queue.clear()

            if file.overwrite:
                file.file.close()
                files_to_delete.append(file_path)
                continue

            # Flush if the flush time is up
            self._flush(file)

        for file_path in files_to_delete:
            del self._open_csv_files[file_path]

    def __del__(self):
        self.store_loop()

        # Close all open files
        for file in self._open_csv_files.values():
            if file.file.closed:
                continue
            file.file.close()

        return super().__del__()
