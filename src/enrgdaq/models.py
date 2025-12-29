import logging
from copy import deepcopy
from enum import Enum
from typing import Optional

from msgspec import Struct


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


class SupervisorCNCConfig(Struct):
    """
    Configuration for the Command and Control (C&C) system.

    Attributes:
        verbosity (LogVerbosity): The verbosity level for logging.
        is_server (bool): Whether this supervisor instance is the C&C server.
        server_host (str): The hostname or IP address of the C&C server.
        rest_api_enabled (bool): Whether to enable the REST API server.
        rest_api_host (str): The hostname or IP address for the REST API server.
        rest_api_port (int): The port for the REST API server.
    """

    verbosity: LogVerbosity = LogVerbosity.INFO

    is_server: bool = False
    server_host: str = "localhost"
    server_port: int = 1638
    rest_api_enabled: bool = False
    rest_api_host: str = "localhost"
    rest_api_port: int = 8000


class SupervisorConfig(Struct, kw_only=True):
    """
    A configuration class for a supervisor.
    Attributes:
        verbosity (LogVerbosity): The verbosity level for logging.
        info (SupervisorInfo): The information of the supervisor.
        cnc (SupervisorCNCConfig | None): The configuration for the Command and Control (C&C) system.
        ring_buffer_size_mb (int): Size of the shared memory ring buffer in MB for zero-copy message transfer.
        ring_buffer_slot_size_kb (int): Size of each slot in the ring buffer in KB.
    """

    verbosity: LogVerbosity = LogVerbosity.INFO
    info: "SupervisorInfo"
    cnc: Optional[SupervisorCNCConfig] = None

    # Ring buffer configuration for zero-copy PyArrow message transfer
    ring_buffer_size_mb: int = 256  # 256 MB total buffer
    ring_buffer_slot_size_kb: int = 10 * 1024  # 1 MB per slot

    def clone(self):
        return deepcopy(self)


class SupervisorInfo(Struct):
    """
    A class to represent the information of a supervisor.
    Attributes:
        supervisor_id (str): The unique identifier for the supervisor.
    """

    supervisor_id: str
    supervisor_tags: list[str] = []


class RestartScheduleInfo(Struct):
    """Information about a restart schedule."""

    job: str
    restart_at: str
