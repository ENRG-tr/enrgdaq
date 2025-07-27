import time
from datetime import datetime

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    StorableDAQJobConfig,
)

DAQ_JOB_BENCHMARK_SLEEP_INTERVAL_SECONDS = 0.01


class DAQJobBenchmarkConfig(StorableDAQJobConfig):
    """
    Configuration for DAQJobBenchmark.

    Attributes:
        payload_size (int): Number of random integers per message.
    """

    payload_size: int = 1000  # default to 1000 values per message


class DAQJobBenchmark(DAQJob):
    """
    Benchmark job for DAQ system to stress test serialization/deserialization/networking.

    Attributes:
        config_type (type): The configuration class type.
        config (DAQJobBenchmarkConfig): The configuration instance.
    """

    config_type = DAQJobBenchmarkConfig
    config: DAQJobBenchmarkConfig

    def __init__(self, config: DAQJobBenchmarkConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._payload = [0 for _ in range(self.config.payload_size)]

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
        timestamp = float(datetime.now().timestamp() * 1000)
        data_row = [timestamp] + self._payload
        keys = ["timestamp"] + [f"V{i}" for i in range(self.config.payload_size)]
        self._put_message_out(
            DAQJobMessageStoreTabular(
                store_config=self.config.store_config,
                keys=keys,
                data=[data_row],  # type: ignore
            )
        )
