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
    """
    Enum representing the verbosity levels for logging.

    Used in DAQJobConfig.
    """

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"

    def to_logging_level(self) -> int:
        return logging._nameToLevel[self.value]


@dataclass
class DAQJobInfo:
    """
    A class to represent the information of a DAQJob.
    Attributes:
        daq_job_type : str
            The type of the DAQ job.
        daq_job_class_name : str
            The class name of the DAQ job, typically the name of the class itself.
        unique_id : str
            A unique identifier for the DAQ job.
        instance_id : int
            An instance identifier for the DAQ job.
        supervisor_config : Optional[SupervisorConfig]
            Configuration for the supervisor, if any.
    """

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
    """
    Configuration for remote communication of DAQJobMessages.

    Used both in DAQJobConfig and in DAQJobMessageStore.

    Attributes:
        remote_topic (Optional[str]): The topic to send the message to for remote communication.
        remote_disable (Optional[bool]): Whether to disable remote communication.
    """

    remote_topic: Optional[str] = DEFAULT_REMOTE_TOPIC
    remote_disable: Optional[bool] = False


class DAQJobConfig(Struct, kw_only=True):
    """
    DAQJobConfig is the base configuration class for DAQJobs.
    Attributes:
        verbosity (LogVerbosity): The verbosity level for logging. Defaults to LogVerbosity.INFO.
        remote_config (Optional[DAQRemoteConfig]): The remote configuration for the DAQ job. Defaults to an instance of DAQRemoteConfig.
        daq_job_type (str): The type of the DAQ job.
    """

    verbosity: LogVerbosity = LogVerbosity.INFO
    remote_config: Optional[DAQRemoteConfig] = field(default_factory=DAQRemoteConfig)
    daq_job_type: str


class DAQJobMessage(Struct, kw_only=True):
    """
    DAQJobMessage is the base class for messages sent between DAQJobs.
    Attributes:
        id (Optional[str]): The unique identifier for the message. Defaults to a UUID.
        timestamp (Optional[datetime]): The timestamp for the message. Defaults to the current datetime.
        is_remote (bool): Whether the message is sent by a remote DAQJob. Defaults to False.
        daq_job_info (Optional[DAQJobInfo]): The information about the DAQJob that sent the message. Defaults to None.
        remote_config (DAQRemoteConfig): The remote configuration for the DAQ job. Defaults to an instance of DAQRemoteConfig.
    """

    id: Optional[str] = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: Optional[datetime] = field(default_factory=datetime.now)
    is_remote: bool = False
    daq_job_info: Optional["DAQJobInfo"] = None
    remote_config: DAQRemoteConfig = field(default_factory=DAQRemoteConfig)


class DAQJobMessageStop(DAQJobMessage):
    reason: str


class DAQJobStatsRecord(Struct):
    """
    A class to represent a record of statistics for a DAQJob.

    Attributes:
        count (int): The number of times the DAQJob has been called.
        last_updated (Optional[datetime]): The last time the DAQJob was called.
    """

    count: int = 0
    last_updated: Optional[datetime] = None

    def increase(self, amount: int = 1):
        self.count += amount
        self.last_updated = datetime.now()


class DAQJobStats(Struct):
    """
    A class to represent statistics for a DAQJob. Gets created and updated by Supervisor.

    Attributes:
        message_in_stats (DAQJobStatsRecord): The statistics for incoming messages.
        message_out_stats (DAQJobStatsRecord): The statistics for outgoing messages.
        restart_stats (DAQJobStatsRecord): The statistics for restarts.
    """

    message_in_stats: DAQJobStatsRecord = field(default_factory=DAQJobStatsRecord)
    message_out_stats: DAQJobStatsRecord = field(default_factory=DAQJobStatsRecord)
    restart_stats: DAQJobStatsRecord = field(default_factory=DAQJobStatsRecord)


class DAQJobStopError(Exception):
    def __init__(self, reason: str):
        self.reason = reason
