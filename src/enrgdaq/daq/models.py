import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from msgspec import Struct, field

from enrgdaq.models import LogVerbosity, SupervisorInfo

DEFAULT_REMOTE_TOPIC = "DAQ"

# Don't send messages to remote if the topic is this
REMOTE_TOPIC_VOID = "@void_message"


@dataclass
class DAQJobInfo:
    """
    A class to represent the information of a DAQJob.
    Attributes:
        daq_job_type : str
            The type of the DAQ job.
        unique_id : str
            A unique identifier for the DAQ job.
        instance_id : int
            An instance identifier for the DAQ job.
        supervisor_config : Optional[SupervisorConfig]
            Configuration for the supervisor, if any.
    """

    daq_job_type: str  # has type(self).__name__
    unique_id: str
    instance_id: int
    config: str
    supervisor_info: Optional[SupervisorInfo] = None

    @staticmethod
    def mock() -> "DAQJobInfo":
        return DAQJobInfo(
            daq_job_type="mock",
            unique_id="mock",
            instance_id=0,
            supervisor_info=SupervisorInfo(supervisor_id="mock"),
            config="",
        )


class DAQRemoteConfig(Struct, kw_only=True):
    """
    Configuration for remote communication of DAQJobMessages.

    Used both in DAQJobConfig and in DAQJobMessageStore.

    Attributes:
        remote_topic (Optional[str]): The topic to send the message to for remote communication.
        remote_disable (Optional[bool]): Whether to send messages from this DAQ job to remote. If True, messages will not be sent to any remote.
        drop_remote_messages (Optional[bool]): Whether to drop remote messages. If True, messages from remote will not be processed, but messages may still be sent to remote.
    """

    remote_topic: Optional[str] = DEFAULT_REMOTE_TOPIC
    remote_disable: Optional[bool] = False
    drop_remote_messages: Optional[bool] = False


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
    daq_job_unique_id: Optional[str] = None
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

    @property
    def supervisor_id(self) -> str:
        if self.daq_job_info is None or self.daq_job_info.supervisor_info is None:
            return "unknown"

        return self.daq_job_info.supervisor_info.supervisor_id


class SHMHandle(Struct):
    shm_name: str
    shm_size: int


class DAQJobMessageSHM(DAQJobMessage):
    shm: SHMHandle


class DAQJobMessageStop(DAQJobMessage):
    reason: str


class DAQJobMessageHeartbeat(DAQJobMessage):
    pass


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
        self.set(self.count + amount)

    def set(self, amount: int):
        self.count = amount
        self.last_updated = datetime.now()


class DAQJobStats(Struct):
    """
    A class to represent statistics for a DAQJob. Gets created and updated by Supervisor.

    Attributes:
        message_in_stats (DAQJobStatsRecord): The statistics for incoming messages.
        message_out_stats (DAQJobStatsRecord): The statistics for outgoing messages.
        restart_stats (DAQJobStatsRecord): The statistics for restarts.
        is_alive (bool): Whether the DAQJob is alive.
    """

    message_in_stats: DAQJobStatsRecord = field(default_factory=DAQJobStatsRecord)
    message_out_stats: DAQJobStatsRecord = field(default_factory=DAQJobStatsRecord)
    message_in_queue_stats: DAQJobStatsRecord = field(default_factory=DAQJobStatsRecord)
    message_out_queue_stats: DAQJobStatsRecord = field(
        default_factory=DAQJobStatsRecord
    )
    restart_stats: DAQJobStatsRecord = field(default_factory=DAQJobStatsRecord)
    is_alive: bool = True


class DAQJobStopError(Exception):
    def __init__(self, reason: str):
        self.reason = reason
