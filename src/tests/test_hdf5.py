import os
import shutil
import unittest
from datetime import datetime

import h5py
import numpy as np

from enrgdaq.daq.jobs.store.hdf5 import DAQJobStoreHDF5, DAQJobStoreHDF5Config
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    DAQJobStoreConfig,
    DAQJobStoreConfigHDF5,
)


class TestHDF5Store(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_hdf5_output"
        self.test_file = os.path.join(self.test_dir, "test.h5")
        self.test_file_real_path = os.path.join("out", self.test_file)
        self.config = DAQJobStoreHDF5Config(daq_job_type="DAQJobStoreHDF5")
        self.hdf5_store = DAQJobStoreHDF5(self.config)
        os.makedirs(self.test_dir, exist_ok=True)

    def tearDown(self):
        # Properly close the file handles before trying to remove the directory
        self.hdf5_store.__del__()
        if os.path.exists(os.path.dirname(self.test_file_real_path)):
            shutil.rmtree(os.path.dirname(self.test_file_real_path))

    def test_handle_message(self):
        store_config = DAQJobStoreConfig(
            hdf5=DAQJobStoreConfigHDF5(
                file_path=self.test_file, add_date=False, dataset_name="test_data"
            )
        )
        message = DAQJobMessageStoreTabular(
            store_config=store_config,
            keys=["col1", "col2"],
            data_columns={"col1": np.array([1, 2]), "col2": np.array([3, 4])},
        )

        self.assertTrue(self.hdf5_store.handle_message(message))

        # Verify data is written correctly
        with h5py.File(self.test_file_real_path, "r") as f:
            self.assertIn("test_data", f)
            self.assertIn("col1", f["test_data"])
            self.assertIn("col2", f["test_data"])
            self.assertTrue(np.array_equal(f["test_data/col1"][:], np.array([1, 2])))
            self.assertTrue(np.array_equal(f["test_data/col2"][:], np.array([3, 4])))

        # Test extending the dataset
        message_2 = DAQJobMessageStoreTabular(
            store_config=store_config,
            keys=["col1", "col2"],
            data_columns={"col1": np.array([5, 6]), "col2": np.array([7, 8])},
        )
        self.assertTrue(self.hdf5_store.handle_message(message_2))

        with h5py.File(self.test_file_real_path, "r") as f:
            self.assertTrue(
                np.array_equal(f["test_data/col1"][:], np.array([1, 2, 5, 6]))
            )
            self.assertTrue(
                np.array_equal(f["test_data/col2"][:], np.array([3, 4, 7, 8]))
            )

    def test_directory_creation(self):
        dir_path = os.path.join(self.test_dir, "new_dir")
        file_path = os.path.join(dir_path, "test.h5")
        store_config = DAQJobStoreConfig(
            hdf5=DAQJobStoreConfigHDF5(
                file_path=file_path, add_date=False, dataset_name="test_data"
            )
        )
        message = DAQJobMessageStoreTabular(
            store_config=store_config,
            keys=["col1"],
            data_columns={"col1": np.array([1])},
        )

        real_file_path = os.path.join("out", file_path)
        real_dir_path = os.path.dirname(real_file_path)

        self.assertFalse(os.path.exists(real_dir_path))
        self.assertTrue(self.hdf5_store.handle_message(message))
        self.assertTrue(os.path.exists(real_file_path))

    def test_file_path_modification(self):
        date_str = datetime.now().strftime("%Y-%m-%d")
        tag = "run1"
        base_file_path = os.path.join(self.test_dir, "tagged_data.h5")
        expected_file_path = os.path.join(
            "out", self.test_dir, f"{date_str}_tagged_data_{tag}.h5"
        )

        store_config = DAQJobStoreConfig(
            hdf5=DAQJobStoreConfigHDF5(
                file_path=base_file_path, add_date=True, dataset_name="test_data"
            )
        )
        message = DAQJobMessageStoreTabular(
            store_config=store_config,
            tag=tag,
            keys=["col1"],
            data_columns={"col1": np.array([1])},
        )

        self.assertTrue(self.hdf5_store.handle_message(message))
        self.assertTrue(os.path.exists(expected_file_path))

    def test_empty_data(self):
        store_config = DAQJobStoreConfig(
            hdf5=DAQJobStoreConfigHDF5(
                file_path=self.test_file, add_date=False, dataset_name="test_data"
            )
        )
        message = DAQJobMessageStoreTabular(
            store_config=store_config, keys=[], data_columns={}
        )

        self.assertTrue(self.hdf5_store.handle_message(message))
        # File should be created, but no datasets
        self.assertTrue(os.path.exists(self.test_file_real_path))
        with h5py.File(self.test_file_real_path, "r") as f:
            self.assertEqual(len(f.keys()), 0)

    def test_multiple_datasets(self):
        store_config1 = DAQJobStoreConfig(
            hdf5=DAQJobStoreConfigHDF5(
                file_path=self.test_file, add_date=False, dataset_name="dataset1"
            )
        )
        message1 = DAQJobMessageStoreTabular(
            store_config=store_config1,
            keys=["a"],
            data_columns={"a": np.array([1])},
        )
        self.assertTrue(self.hdf5_store.handle_message(message1))

        store_config2 = DAQJobStoreConfig(
            hdf5=DAQJobStoreConfigHDF5(
                file_path=self.test_file, add_date=False, dataset_name="dataset2"
            )
        )
        message2 = DAQJobMessageStoreTabular(
            store_config=store_config2,
            keys=["b"],
            data_columns={"b": np.array([2])},
        )
        self.assertTrue(self.hdf5_store.handle_message(message2))

        with h5py.File(self.test_file_real_path, "r") as f:
            self.assertIn("dataset1", f)
            self.assertIn("dataset2", f)
            self.assertTrue(np.array_equal(f["dataset1/a"][:], np.array([1])))
            self.assertTrue(np.array_equal(f["dataset2/b"][:], np.array([2])))


if __name__ == "__main__":
    unittest.main()
