import time
from dataclasses import dataclass
from random import randint

from N1081B import N1081B

from daq.base import DAQJob
from daq.store.models import DAQJobMessageStore, StorableDAQJobConfig


@dataclass
class DAQJobTestConfig(StorableDAQJobConfig):
    rand_min: int
    rand_max: int


class DAQJobTest(DAQJob):
    config_type = DAQJobTestConfig
    device: N1081B
    config: DAQJobTestConfig

    def start(self):
        while True:
            self.consume()
            self._send_store_message()

            time.sleep(1)

    def _send_store_message(self):
        def get_int():
            return randint(self.config.rand_min, self.config.rand_max)

        self.message_out.put(
            DAQJobMessageStore(
                store_config=self.config.store_config,
                daq_job=self,
                keys=["A", "B", "C"],
                data=[[get_int(), get_int(), get_int()]],
            )
        )
