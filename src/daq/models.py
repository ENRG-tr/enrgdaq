import logging
import threading
from dataclasses import dataclass
from typing import Any

from dataclasses_json import DataClassJsonMixin


@dataclass
class DAQJobConfig(DataClassJsonMixin):
    daq_job_type: str


class DAQJob:
    config_type: Any
    config: Any
    logger: logging.Logger
    _should_stop: bool

    def __init__(self, config: Any):
        self.config = config
        self.logger = logging.getLogger(type(self).__name__)
        self._should_stop = False

    def start(self):
        pass

    def stop(self):
        self._should_stop = True


@dataclass
class DAQJobThread:
    daq_job: DAQJob
    thread: threading.Thread
