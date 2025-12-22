import time
from datetime import datetime

import numpy as np

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    StorableDAQJobConfig,
)

DAQ_JOB_BENCHMARK_SLEEP_INTERVAL_SECONDS = 0.001  # 1ms between messages


class DAQJobBenchmarkConfig(StorableDAQJobConfig):
    """
    Configuration for DAQJobBenchmark.

    Attributes:
        payload_size (int): Number of values per message.
        use_shm (bool): Use shared memory for message passing.
    """

    payload_size: int = 1000  # default to 1000 values per message
    use_shm: bool = False  # Use shared memory for large payloads


class DAQJobBenchmark(DAQJob):
    """
    Benchmark job for DAQ system to stress test serialization/deserialization/networking.

    Optimization: Stores numpy data as raw bytes in the data field. This is MUCH faster
    to pickle than Python lists because:
    1. numpy.tobytes() is just a memcpy (C-level, no Python object overhead)
    2. Pickling a single bytes object is much faster than 100k+ Python floats
    """

    config_type = DAQJobBenchmarkConfig
    config: DAQJobBenchmarkConfig

    def __init__(self, config: DAQJobBenchmarkConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._payload_size = self.config.payload_size
        # Pre-allocate numpy array for the payload (reused each message)
        # First element is timestamp, rest is data
        self._data_array = np.zeros(1 + self._payload_size, dtype=np.float64)
        # Pre-compute keys once
        self._keys = ["timestamp"] + [f"V{i}" for i in range(self._payload_size)]

    def start(self):
        """
        Start the benchmark job, sending as much data as possible.
        """
        while True:
            self._send_store_message()
            time.sleep(DAQ_JOB_BENCHMARK_SLEEP_INTERVAL_SECONDS)

    def _send_store_message(self):
        """
        Send a store message with a large payload.
        """
        # Update timestamp in pre-allocated array
        self._data_array[0] = datetime.now().timestamp() * 1000  # timestamp in ms

        # Convert numpy array to bytes
        data_bytes = self._data_array.tobytes()

        self._put_message_out(
            DAQJobMessageStoreTabular(
                store_config=self.config.store_config,
                keys=self._keys,
                data=[data_bytes],
            ),
            use_shm=self.config.use_shm,
        )
