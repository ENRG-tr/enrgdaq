from dataclasses import dataclass
from typing import Any

from dataclasses_json import DataClassJsonMixin


@dataclass
class DAQJobConfig(DataClassJsonMixin):
    daq_job_type: str


class DAQJob:
    config_type: Any
    _should_stop: bool
    config: Any

    def __init__(self, config: Any):
        self.config = config

    def start(self):
        pass

    def stop(self):
        self._should_stop = True
