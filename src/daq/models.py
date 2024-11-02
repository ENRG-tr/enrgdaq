import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

from dataclasses_json import DataClassJsonMixin


class LogVerbosity(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"

    def to_logging_level(self) -> int:
        return logging._nameToLevel[self.value]


@dataclass(kw_only=True)
class DAQJobConfig(DataClassJsonMixin):
    verbosity: LogVerbosity = LogVerbosity.INFO
    daq_job_type: str


@dataclass(kw_only=True)
class DAQJobMessage(DataClassJsonMixin):
    id: Optional[str] = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: Optional[datetime] = field(default_factory=datetime.now)
    is_remote: bool = False


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
