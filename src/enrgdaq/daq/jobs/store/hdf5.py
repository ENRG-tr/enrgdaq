import os
from typing import Any, cast

import h5py

from enrgdaq.daq.models import DAQJobConfig
from enrgdaq.daq.store.base import DAQJobStore
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    DAQJobStoreConfigHDF5,
)
from enrgdaq.utils.file import modify_file_path


class DAQJobStoreHDF5Config(DAQJobConfig):
    out_dir: str = "out/"


class DAQJobStoreHDF5(DAQJobStore):
    config_type = DAQJobStoreHDF5Config
    allowed_store_config_types = [DAQJobStoreConfigHDF5]
    allowed_message_in_types = [DAQJobMessageStoreTabular]
    _open_files: dict[str, Any]

    def __init__(self, config: Any, **kwargs):
        super().__init__(config, **kwargs)

        self._open_files = {}

    def store_loop(self):
        pass

    def handle_message(self, message: DAQJobMessageStoreTabular) -> bool:
        super().handle_message(message)

        if message.data_columns is None:
            return True

        store_config = cast(DAQJobStoreConfigHDF5, message.store_config.hdf5)
        file_path = modify_file_path(
            store_config.file_path, store_config.add_date, message.tag
        )
        file_path = os.path.join(self.config.out_dir, file_path)

        if file_path not in self._open_files:
            # Ensure directory exists to prevent errors
            dir_name = os.path.dirname(file_path)
            if dir_name:
                os.makedirs(dir_name, exist_ok=True)

            hdf5_file = h5py.File(file_path, "a")
            self._open_files[file_path] = hdf5_file
            self._logger.info(f"Opened file {file_path}")
        else:
            hdf5_file = self._open_files[file_path]

        dataset_name = store_config.dataset_name
        data_to_write = message.data_columns

        if not data_to_write:
            return True

        if dataset_name not in hdf5_file:
            # Create a resizable dataset for each column
            for k, v in data_to_write.items():
                hdf5_file.create_dataset(
                    f"{dataset_name}/{k}",
                    data=v,
                    maxshape=(None,),
                    chunks=True,
                )
        else:
            for k, v in data_to_write.items():
                dataset = hdf5_file[f"{dataset_name}/{k}"]
                assert isinstance(dataset, h5py.Dataset), "Dataset is not a Dataset"
                dataset.resize((dataset.shape[0] + len(v)), axis=0)
                dataset[-len(v) :] = v

        return True

    def __del__(self):
        # Ensure all files are properly closed on exit.
        for hdf5_file in self._open_files.values():
            hdf5_file.close()
        self._open_files.clear()

        super().__del__()
