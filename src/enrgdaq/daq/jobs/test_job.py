import time
from datetime import datetime
from random import randint

from pyarrow.ipc import pa

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.store.models import (
    DAQJobMessageStorePyArrow,
    StorableDAQJobConfig,
)


class DAQJobTestConfig(StorableDAQJobConfig):
    """
    Configuration class for DAQJobTest.

    Attributes:
        rand_min (int): Minimum random integer value.
        rand_max (int): Maximum random integer value.
    """

    rand_min: int
    rand_max: int


class DAQJobTest(DAQJob):
    """
    A test job for the DAQ system that generates random data.

    Attributes:
        config_type (type): The configuration class type.
        config (DAQJobTestConfig): The configuration instance.
    """

    config_type = DAQJobTestConfig
    config: DAQJobTestConfig

    def start(self):
        """
        Start the DAQ job, continuously consuming data and sending store messages.
        """
        while not self._has_been_freed:
            # self.consume()
            self._send_store_message()

            time.sleep(1)

    def _send_store_message(self):
        """
        Send a store message with random data.
        """

        def get_int():
            """
            Generate a random integer within the configured range.

            Returns:
                int: A random integer.
            """
            return randint(self.config.rand_min, self.config.rand_max)

        self._put_message_out(
            DAQJobMessageStorePyArrow(
                store_config=self.config.store_config,
                table=pa.Table.from_arrays(
                    [
                        pa.array([int(datetime.now().timestamp() * 1000)]),
                        pa.array([get_int()]),
                        pa.array([get_int()]),
                        pa.array([get_int()]),
                    ],
                    names=["timestamp", "A", "B", "C"],
                ),
            ),
            use_shm=True,
        )
