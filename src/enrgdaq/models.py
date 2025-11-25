from copy import deepcopy
from typing import Optional

from msgspec import Struct


class SupervisorCNCConfig(Struct):
    """
    Configuration for the Command and Control (C&C) system.

    Attributes:
        is_server (bool): Whether this supervisor instance is the C&C server.
        server_host (str): The hostname or IP address of the C&C server.
        rest_api_enabled (bool): Whether to enable the REST API server.
        rest_api_host (str): The hostname or IP address for the REST API server.
        rest_api_port (int): The port for the REST API server.
    """

    is_server: bool = False
    server_host: str = "localhost"
    server_port: int = 1638
    rest_api_enabled: bool = False
    rest_api_host: str = "localhost"
    rest_api_port: int = 8000


class SupervisorConfig(Struct):
    """
    A configuration class for a supervisor.
    Attributes:
        supervisor_id (str): The unique identifier for the supervisor that is going to be used primarily for DAQJobRemote.
    """

    info: "SupervisorInfo"
    cnc: Optional[SupervisorCNCConfig] = None

    def clone(self):
        return deepcopy(self)


class SupervisorInfo(Struct):
    """
    A class to represent the information of a supervisor.
    Attributes:
        supervisor_id (str): The unique identifier for the supervisor.
    """

    supervisor_id: str


class RestartScheduleInfo(Struct):
    """Information about a restart schedule."""

    job: str
    restart_at: str
