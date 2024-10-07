from dataclasses import dataclass
from typing import Any

from dataclasses_json import DataClassJsonMixin


@dataclass
class DAQJobConfig(DataClassJsonMixin):
    daq_job_type: str


class DAQJob:
    config_type: Any
    config: Any
    _should_stop: bool

    def __init__(self, config: Any):
        self.config = config
        self._should_stop = False

    def start(self):
        pass

    def stop(self):
        self._should_stop = True
