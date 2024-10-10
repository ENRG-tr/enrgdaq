from dataclasses import dataclass

from dataclasses_json import DataClassJsonMixin


@dataclass
class DAQJobConfig(DataClassJsonMixin):
    daq_job_type: str


@dataclass
class DAQJobMessage:
    pass


@dataclass
class DAQJobMessageStop(DAQJobMessage):
    reason: str


class DAQJobStopError(Exception):
    def __init__(self, reason: str):
        self.reason = reason
