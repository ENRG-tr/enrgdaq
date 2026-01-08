import time
from datetime import datetime

import numpy as np
import pyarrow as pa

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.store.models import (
    DAQJobMessageStorePyArrow,
    StorableDAQJobConfig,
)

DAQ_JOB_BENCHMARK_SLEEP_INTERVAL_SECONDS = 0.001  # 1ms between messages


class DAQJobBenchmarkConfig(StorableDAQJobConfig):
    """
    Configuration for DAQJobBenchmark.

    Attributes:
        payload_size (int): Number of rows per message.
        use_shm (bool): Use shared memory for message passing.
    """

    payload_size: int = 1000
    use_shm: bool = False


class DAQJobBenchmark(DAQJob):
    """
    Benchmark job for DAQ system to stress test serialization/deserialization/networking.
    """

    config_type = DAQJobBenchmarkConfig
    config: DAQJobBenchmarkConfig

    def __init__(self, config: DAQJobBenchmarkConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._payload_size = self.config.payload_size
        # Pre-allocate numpy array for the payload (reused each message)
        self._data_array = np.zeros(self._payload_size, dtype=np.float64)

    def start(self):
        """
        Start the benchmark job, sending as much data as possible.
        """
        i = 0
        while True:
            # Only check for messages every 1000 iterations to maximize production speed
            # if i % 1000 == 0:
                # self.consume()

            self._send_store_message()
            i += 1
            time.sleep(0.00005)

    def _send_store_message(self):
        """
        Send a store message with a large payload using PyArrow.
        Uses columnar layout: many rows, few columns.
        """
        timestamp = datetime.now().timestamp() * 1000  # timestamp in ms

        # Create PyArrow table with many rows
        # timestamp column: same timestamp for all rows in this batch
        # value column: the actual payload data
        table = pa.table(
            {
                "timestamp": pa.array(
                    np.full(self._payload_size, timestamp, dtype=np.float64)
                ),
                "value": pa.array(np.random.random(self._payload_size)),
            }
        )

        self._put_message_out(
            DAQJobMessageStorePyArrow(
                store_config=self.config.store_config,
                table=table,
            ),
            use_shm=self.config.use_shm,
        )
