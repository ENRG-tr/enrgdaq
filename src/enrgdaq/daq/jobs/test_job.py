import time
from datetime import datetime
from random import randint

from N1081B import N1081B

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.store.models import DAQJobMessageStore, StorableDAQJobConfig


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
        device (N1081B): The device used for data acquisition.
        config (DAQJobTestConfig): The configuration instance.
    """

    config_type = DAQJobTestConfig
    device: N1081B
    config: DAQJobTestConfig

    def start(self):
        """
        Start the DAQ job, continuously consuming data and sending store messages.
        """
        while True:
            self.consume()
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
            DAQJobMessageStore(
                store_config=self.config.store_config,
                keys=["timestamp", "A", "B", "C"],
                data=[
                    [datetime.now().timestamp() * 1000, get_int(), get_int(), get_int()]
                ],
            )
        )
