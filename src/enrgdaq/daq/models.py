import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

from msgspec import Struct, field

from enrgdaq.models import SupervisorConfig

DEFAULT_REMOTE_TOPIC = "DAQ"

# Don't send messages to remote if the topic is this
REMOTE_TOPIC_VOID = "@void_message"


class LogVerbosity(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"

    def to_logging_level(self) -> int:
        return logging._nameToLevel[self.value]


@dataclass
class DAQJobInfo:
    daq_job_type: str
    daq_job_class_name: str  # has type(self).__name__
    unique_id: str
    instance_id: int
    supervisor_config: Optional[SupervisorConfig] = None

    @staticmethod
    def mock() -> "DAQJobInfo":
        return DAQJobInfo(
            daq_job_type="mock",
            daq_job_class_name="mock",
            unique_id="mock",
            instance_id=0,
            supervisor_config=SupervisorConfig(supervisor_id="mock"),
        )


class DAQRemoteConfig(Struct, kw_only=True):
    remote_topic: Optional[str] = DEFAULT_REMOTE_TOPIC
    remote_disable: Optional[bool] = False


class DAQJobConfig(Struct, kw_only=True):
    verbosity: LogVerbosity = LogVerbosity.INFO
    remote_config: Optional[DAQRemoteConfig] = field(default_factory=DAQRemoteConfig)
    daq_job_type: str


class DAQJobMessage(Struct, kw_only=True):
    id: Optional[str] = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: Optional[datetime] = field(default_factory=datetime.now)
    is_remote: bool = False
    daq_job_info: Optional["DAQJobInfo"] = None
    remote_config: DAQRemoteConfig = field(default_factory=DAQRemoteConfig)


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
