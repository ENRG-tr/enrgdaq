import time

try:
    from typing import override  # type: ignore
except ImportError:

    def override(func):
        return func


from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import DAQJobConfig

# Sleep interval when no messages are available (prevents busy-waiting)
STORE_IDLE_SLEEP_SECONDS = 0.001


class DAQJobStore(DAQJob):
    """
    DAQJobStore is an abstract base class for data acquisition job stores.
    """

    def __init__(self, config: DAQJobConfig, **kwargs):
        self.topics_to_subscribe.append("store." + type(self).__name__)
        super().__init__(config, **kwargs)

    @override
    def start(self):
        """
        Starts the continuous loop for consuming and storing data.
        """
        while not self._has_been_freed:
            self.store_loop()
            time.sleep(STORE_IDLE_SLEEP_SECONDS)

    def store_loop(self):
        pass
