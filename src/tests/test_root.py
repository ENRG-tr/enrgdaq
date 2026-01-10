import os
import shutil
import unittest
from datetime import datetime

import numpy as np
import uproot

from enrgdaq.daq.jobs.store.root import DAQJobStoreROOT, DAQJobStoreROOTConfig
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    DAQJobStoreConfig,
    DAQJobStoreConfigROOT,
)
from enrgdaq.models import SupervisorInfo


class TestROOTStore(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_root_output"
        self.test_file = os.path.join(self.test_dir, "test.root")
        self.test_file_real_path = os.path.join("out", self.test_file)
        self.config = DAQJobStoreROOTConfig(daq_job_type="DAQJobStoreROOT")
        self.supervisor_info = SupervisorInfo(supervisor_id="test")
        self.root_store = DAQJobStoreROOT(
            self.config, supervisor_info=self.supervisor_info
        )
        os.makedirs(os.path.join("out", self.test_dir), exist_ok=True)

    def tearDown(self):
        # Properly close the file handles before trying to remove the directory
        self.root_store.__del__()
        if os.path.exists(os.path.dirname(self.test_file_real_path)):
            shutil.rmtree(os.path.dirname(self.test_file_real_path))

    def test_handle_message(self):
        store_config = DAQJobStoreConfig(
            root=DAQJobStoreConfigROOT(
                file_path=self.test_file, add_date=False, tree_name="test_data"
            )
        )
        message = DAQJobMessageStoreTabular(
            store_config=store_config,
            keys=["col1", "col2"],
            data_columns={"col1": np.array([1, 2]), "col2": np.array([3.0, 4.0])},
        )

        self.assertTrue(self.root_store.handle_message(message))
        self.root_store._flush_all_buffers()  # Flush buffered data to disk

        # Verify data is written correctly
        with uproot.open(self.test_file_real_path) as f:
            self.assertIn("test_data", f)
            tree = f["test_data"]
            self.assertIn("col1", tree.keys())
            self.assertIn("col2", tree.keys())
            arrays = tree.arrays(library="np")
            self.assertTrue(np.array_equal(arrays["col1"], np.array([1, 2])))
            self.assertTrue(np.allclose(arrays["col2"], np.array([3.0, 4.0])))

        # Test extending the tree
        message_2 = DAQJobMessageStoreTabular(
            store_config=store_config,
            keys=["col1", "col2"],
            data_columns={"col1": np.array([5, 6]), "col2": np.array([7.0, 8.0])},
        )
        self.assertTrue(self.root_store.handle_message(message_2))
        self.root_store._flush_all_buffers()  # Flush buffered data to disk

        with uproot.open(self.test_file_real_path) as f:
            keys = f.keys()
            col1_combined = np.concatenate([f[k]["col1"] for k in keys])
            col2_combined = np.concatenate([f[k]["col2"] for k in keys])

            arrays = tree.arrays(library="np")
            self.assertTrue(np.array_equal(col1_combined, np.array([1, 2, 5, 6])))
            self.assertTrue(np.allclose(col2_combined, np.array([3.0, 4.0, 7.0, 8.0])))

    def test_directory_creation(self):
        dir_path = os.path.join(self.test_dir, "new_dir")
        file_path = os.path.join(dir_path, "test.root")
        store_config = DAQJobStoreConfig(
            root=DAQJobStoreConfigROOT(
                file_path=file_path, add_date=False, tree_name="test_data"
            )
        )
        message = DAQJobMessageStoreTabular(
            store_config=store_config,
            keys=["col1"],
            data_columns={"col1": np.array([1])},
        )

        real_file_path = os.path.join("out", file_path)
        real_dir_path = os.path.dirname(real_file_path)

        # Ensure clean state for this test
        if os.path.exists(real_dir_path):
            shutil.rmtree(real_dir_path)

        self.assertFalse(os.path.exists(real_dir_path))
        self.assertTrue(self.root_store.handle_message(message))
        self.root_store._flush_all_buffers()  # Flush buffered data to disk
        self.assertTrue(os.path.exists(real_file_path))

    def test_file_path_modification(self):
        date_str = datetime.now().strftime("%Y-%m-%d")
        tag = "run1"
        base_file_path = os.path.join(self.test_dir, "tagged_data.root")
        expected_file_path = os.path.join(
            "out", self.test_dir, f"{date_str}_tagged_data_{tag}.root"
        )

        store_config = DAQJobStoreConfig(
            root=DAQJobStoreConfigROOT(
                file_path=base_file_path, add_date=True, tree_name="test_data"
            )
        )
        message = DAQJobMessageStoreTabular(
            store_config=store_config,
            tag=tag,
            keys=["col1"],
            data_columns={"col1": np.array([1])},
        )

        self.assertTrue(self.root_store.handle_message(message))
        self.root_store._flush_all_buffers()  # Flush buffered data to disk
        self.assertTrue(os.path.exists(expected_file_path))

    def test_empty_data(self):
        store_config = DAQJobStoreConfig(
            root=DAQJobStoreConfigROOT(
                file_path=self.test_file, add_date=False, tree_name="test_data"
            )
        )
        message = DAQJobMessageStoreTabular(
            store_config=store_config, keys=[], data_columns={}
        )

        self.assertTrue(self.root_store.handle_message(message))
        # File should not be created if there's no data to write a tree
        self.assertFalse(os.path.exists(self.test_file_real_path))

    def test_multiple_trees(self):
        store_config1 = DAQJobStoreConfig(
            root=DAQJobStoreConfigROOT(
                file_path=self.test_file, add_date=False, tree_name="tree1"
            )
        )
        message1 = DAQJobMessageStoreTabular(
            store_config=store_config1,
            keys=["a"],
            data_columns={"a": np.array([1])},
        )
        self.assertTrue(self.root_store.handle_message(message1))

        store_config2 = DAQJobStoreConfig(
            root=DAQJobStoreConfigROOT(
                file_path=self.test_file, add_date=False, tree_name="tree2"
            )
        )
        message2 = DAQJobMessageStoreTabular(
            store_config=store_config2,
            keys=["b"],
            data_columns={"b": np.array([2.0])},
        )
        self.assertTrue(self.root_store.handle_message(message2))
        self.root_store._flush_all_buffers()  # Flush buffered data to disk

        with uproot.open(self.test_file_real_path) as f:
            self.assertIn("tree1", f)
            self.assertIn("tree2", f)
            self.assertTrue(
                np.array_equal(f["tree1"]["a"].array(library="np"), np.array([1]))
            )
            self.assertTrue(
                np.allclose(f["tree2"]["b"].array(library="np"), np.array([2.0]))
            )


if __name__ == "__main__":
    unittest.main()
