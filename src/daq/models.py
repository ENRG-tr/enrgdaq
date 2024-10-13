from dataclasses import dataclass
from datetime import datetime
from typing import Optional

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


@dataclass
class DAQJobStats:
    message_in_count: int
    message_out_count: int
    last_message_in_date: Optional[datetime]
    last_message_out_date: Optional[datetime]


class DAQJobStopError(Exception):
    def __init__(self, reason: str):
        self.reason = reason
