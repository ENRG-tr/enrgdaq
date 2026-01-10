import logging
from copy import deepcopy
from enum import Enum

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


class SupervisorFederationConfig(Struct):
    """
    Configuration for federation between Supervisors.

    In a star topology:
    - The server supervisor exposes XPUB/XSUB endpoints that clients connect to
    - Client supervisors connect bidirectionally to the server

    Attributes:
        is_server (bool): Whether this supervisor is the server (central node).
        server_xpub_url (str | None): The XPUB URL exposed by the server.
        server_xsub_url (str | None): The XSUB URL exposed by the server.
        remote_server_xpub_url (str | None): The server's XPUB URL to subscribe to (for clients).
        remote_server_xsub_url (str | None): The server's XSUB URL to publish to (for clients).
    """

    is_server: bool = False
    # Server configuration - URLs to expose
    server_xpub_url: str | None = None
    server_xsub_url: str | None = None
    # Client configuration - URLs to connect to
    remote_server_xpub_url: str | None = None
    remote_server_xsub_url: str | None = None


class SupervisorConfig(Struct, kw_only=True):
    """
    A configuration class for a supervisor.
    Attributes:
        verbosity (LogVerbosity): The verbosity level for logging.
        info (SupervisorInfo): The information of the supervisor.
        cnc (SupervisorCNCConfig | None): The configuration for the Command and Control (C&C) system.
        federation (SupervisorFederationConfig | None): The configuration for cross-supervisor federation.
        ring_buffer_size_mb (int): Size of the shared memory ring buffer in MB for zero-copy message transfer.
        ring_buffer_slot_size_kb (int): Size of each slot in the ring buffer in KB.
    """

    verbosity: LogVerbosity = LogVerbosity.INFO
    info: "SupervisorInfo"
    cnc: SupervisorCNCConfig | None = None
    federation: SupervisorFederationConfig | None = None

    # Ring buffer configuration for zero-copy PyArrow message transfer
    ring_buffer_size_mb: int = 256
    ring_buffer_slot_size_kb: int = 1 * 1024

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
