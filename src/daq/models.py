from dataclasses import dataclass, field
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
class DAQJobStatsRecord:
    count: int
    last_updated: Optional[datetime]

    @staticmethod
    def new() -> "DAQJobStatsRecord":
        return DAQJobStatsRecord(count=0, last_updated=None)

    def increase(self, amount: int = 1):
        self.count += amount
        self.last_updated = datetime.now()


@dataclass
class DAQJobStats:
    message_in_stats: DAQJobStatsRecord = field(
        default_factory=lambda: DAQJobStatsRecord.new()
    )
    message_out_stats: DAQJobStatsRecord = field(
        default_factory=lambda: DAQJobStatsRecord.new()
    )
    restart_stats: DAQJobStatsRecord = field(
        default_factory=lambda: DAQJobStatsRecord.new()
    )


class DAQJobStopError(Exception):
    def __init__(self, reason: str):
        self.reason = reason
