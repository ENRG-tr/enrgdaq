from typing import Optional, Union

from msgspec import Struct

from enrgdaq.daq.jobs.handle_stats import DAQJobStatsDict
from enrgdaq.daq.jobs.remote import DAQJobRemoteStatsDict
from enrgdaq.models import RestartScheduleInfo, SupervisorInfo


class SupervisorStatus(Struct):
    """
    A class to represent the status of a supervisor.
    """

    supervisor_info: SupervisorInfo
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

    clients: dict[str, SupervisorInfo]


class CNCMessageReqUpdateAndRestart(CNCMessage):
    """Request to update and restart ENRGDAQ."""

    pass


class CNCMessageResUpdateAndRestart(CNCMessage):
    """Response to update and restart request."""

    success: bool
    message: str


class CNCMessageReqRestartDAQJobs(CNCMessage):
    """Request to restart DAQJobs."""

    pass


class CNCMessageResRestartDAQJobs(CNCMessage):
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


class CNCMessageReqStopAndRemoveDAQJob(CNCMessage):
    """Request to stop and remove a specific DAQJob by name."""

    daq_job_name: str


class CNCMessageResStopAndRemoveDAQJob(CNCMessage):
    """Response to stop and remove DAQJob request."""

    success: bool
    message: str


CNCMessageType = Union[
    CNCMessageHeartbeat,
    CNCMessageReqPing,
    CNCMessageResPing,
    CNCMessageReqStatus,
    CNCMessageResStatus,
    CNCMessageReqListClients,
    CNCMessageResListClients,
    CNCMessageReqUpdateAndRestart,
    CNCMessageResUpdateAndRestart,
    CNCMessageReqRestartDAQJobs,
    CNCMessageResRestartDAQJobs,
    CNCMessageReqRunCustomDAQJob,
    CNCMessageResRunCustomDAQJob,
    CNCMessageReqStopAndRemoveDAQJob,
    CNCMessageResStopAndRemoveDAQJob,
]

CNC_DEFAULT_PORT = 5555
