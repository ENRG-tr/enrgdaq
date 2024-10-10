import logging
import threading
from dataclasses import dataclass
from queue import Queue
from typing import Any

from daq.models import DAQJobMessage, DAQJobMessageStop, DAQJobStopError


class DAQJob:
    config_type: Any
    config: Any
    message_in: Queue["DAQJobMessage"]
    message_out: Queue["DAQJobMessage"]

    _logger: logging.Logger

    def __init__(self, config: Any):
        self.config = config
        self.message_in = Queue()
        self.message_out = Queue()
        self._logger = logging.getLogger(type(self).__name__)
        self._should_stop = False

    def consume(self):
        # consume messages from the queue
        while not self.message_in.empty():
            message = self.message_in.get()
            if not self.handle_message(message):
                self.message_in.put_nowait(message)

    def handle_message(self, message: "DAQJobMessage") -> bool:
        if isinstance(message, DAQJobMessageStop):
            raise DAQJobStopError(message.reason)
        return True

    def start(self):
        raise NotImplementedError

    def __del__(self):
        self._logger.info("DAQ job is being deleted")


@dataclass
class DAQJobThread:
    daq_job: DAQJob
    thread: threading.Thread
