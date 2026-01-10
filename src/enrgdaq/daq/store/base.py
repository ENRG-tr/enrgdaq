import time

from enrgdaq.models import SupervisorInfo

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

    def __init__(self, config: DAQJobConfig, supervisor_info: SupervisorInfo, **kwargs):
        from enrgdaq.daq.topics import Topic

        self.topics_to_subscribe.append(Topic.store(type(self).__name__))
        self.topics_to_subscribe.append(
            Topic.store_supervisor(supervisor_info.supervisor_id, type(self).__name__)
        )
        super().__init__(config, supervisor_info, **kwargs)

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
