from typing import Union

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


class CNCMessage(Struct, tag=True):
    """Base class for C&C messages."""

    pass


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


CNCMessageType = Union[
    CNCMessageHeartbeat,
    CNCMessageReqPing,
    CNCMessageResPing,
    CNCMessageReqStatus,
    CNCMessageResStatus,
    CNCMessageReqListClients,
    CNCMessageResListClients,
]

CNC_DEFAULT_PORT = 5555
