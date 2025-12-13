from typing import Optional, Union

from msgspec import Struct

from enrgdaq.daq.jobs.handle_stats import DAQJobStatsDict
from enrgdaq.daq.jobs.remote import DAQJobRemoteStatsDict
from enrgdaq.daq.models import DAQJobInfo
from enrgdaq.models import RestartScheduleInfo, SupervisorInfo


class SupervisorStatus(Struct):
    """
    A class to represent the status of a supervisor.
    """

    supervisor_info: SupervisorInfo
    daq_jobs: list[DAQJobInfo]
    daq_job_stats: DAQJobStatsDict
    daq_job_remote_stats: DAQJobRemoteStatsDict
    restart_schedules: list[RestartScheduleInfo]


class CNCMessage(Struct, tag=True, kw_only=True):
    """Base class for C&C messages."""

    req_id: Optional[str] = None


class CNCMessageHeartbeat(CNCMessage):
    """Heartbeat message sent by clients to the server."""

    supervisor_info: SupervisorInfo


class CNCMessageReqPing(CNCMessage):
    """Ping message."""

    pass


class CNCMessageResPing(CNCMessage):
    """Pong message."""

    pass


class CNCMessageReqStatus(CNCMessage):
    """Request for supervisor status."""

    pass


class CNCMessageResStatus(CNCMessage):
    """Supervisor status."""

    status: SupervisorStatus


class CNCMessageReqListClients(CNCMessage):
    """Request to list connected clients."""

    pass


class CNCMessageResListClients(CNCMessage):
    """List of connected clients."""

    clients: dict[str, Optional[SupervisorInfo]]


class CNCMessageReqRestartDAQ(CNCMessage):
    """Request to restart ENRGDAQ."""

    update: bool = False

    pass


class CNCMessageResRestartDAQ(CNCMessage):
    """Response to update and restart request."""

    success: bool
    message: str


class CNCMessageReqStopDAQJobs(CNCMessage):
    """Request to restart DAQJobs."""

    pass


class CNCMessageResStopDAQJobs(CNCMessage):
    """Response to restart DAQJobs request."""

    success: bool
    message: str


class CNCMessageReqRunCustomDAQJob(CNCMessage):
    """Request to run a custom DAQJob with config."""

    config: str  # TOML config as string


class CNCMessageResRunCustomDAQJob(CNCMessage):
    """Response to run custom DAQJob request."""

    success: bool
    message: str


class CNCMessageReqStopDAQJob(CNCMessage):
    """Request to stop and remove a specific DAQJob by name."""

    daq_job_name: Optional[str] = None
    daq_job_unique_id: Optional[str] = None
    remove: bool = False


class CNCMessageResStopDAQJob(CNCMessage):
    """Response to stop and remove DAQJob request."""

    success: bool
    message: str


class CNCMessageReqSendMessage(CNCMessage):
    """Request to send a custom message to DAQ job(s)."""

    message_type: str  # The type name of the DAQJobMessage to send
    payload: str  # JSON-encoded message payload
    target_daq_job_unique_id: Optional[str] = (
        None  # If specified, only send to this job
    )


class CNCMessageResSendMessage(CNCMessage):
    """Response to send custom message request."""

    success: bool
    message: str
    jobs_notified: int = 0


class CNCMessageLog(CNCMessage):
    """Log message from client to server."""

    level: str  # e.g. 'INFO', 'WARNING', 'ERROR'
    message: str
    timestamp: str
    module: str  # module or component that generated the log
    client_id: str  # ID of the client sending the log


class CNCClientInfo(Struct):
    """Information about a connected client."""

    identity: Optional[bytes] = None
    last_seen: str = ""
    info: Optional[SupervisorInfo] = None


CNCMessageType = Union[
    CNCMessageHeartbeat,
    CNCMessageReqPing,
    CNCMessageResPing,
    CNCMessageReqStatus,
    CNCMessageResStatus,
    CNCMessageReqListClients,
    CNCMessageResListClients,
    CNCMessageReqRestartDAQ,
    CNCMessageResRestartDAQ,
    CNCMessageReqStopDAQJobs,
    CNCMessageResStopDAQJobs,
    CNCMessageReqRunCustomDAQJob,
    CNCMessageResRunCustomDAQJob,
    CNCMessageReqStopDAQJob,
    CNCMessageResStopDAQJob,
    CNCMessageReqSendMessage,
    CNCMessageResSendMessage,
    CNCMessageLog,
]
