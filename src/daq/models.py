import logging
import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

from msgspec import Struct, field


class LogVerbosity(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"

    def to_logging_level(self) -> int:
        return logging._nameToLevel[self.value]


class DAQJobConfig(Struct, kw_only=True):
    verbosity: LogVerbosity = LogVerbosity.INFO
    daq_job_type: str


class DAQJobMessage(Struct, kw_only=True):
    id: Optional[str] = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: Optional[datetime] = field(default_factory=datetime.now)
    is_remote: bool = False


class DAQJobMessageStop(DAQJobMessage):
    reason: str


class DAQJobStatsRecord(Struct):
    count: int = 0
    last_updated: Optional[datetime] = None

    def increase(self, amount: int = 1):
        self.count += amount
        self.last_updated = datetime.now()


class DAQJobStats(Struct):
    message_in_stats: DAQJobStatsRecord = field(default_factory=DAQJobStatsRecord)
    message_out_stats: DAQJobStatsRecord = field(default_factory=DAQJobStatsRecord)
    restart_stats: DAQJobStatsRecord = field(default_factory=DAQJobStatsRecord)


class DAQJobStopError(Exception):
    def __init__(self, reason: str):
        self.reason = reason
