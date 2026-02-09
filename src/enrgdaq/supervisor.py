import base64
import hashlib
import logging
import os
import platform
import sys
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cache
from logging.handlers import QueueListener
from typing import Any

import msgspec
import psutil
import zmq

from enrgdaq.cnc.base import (
    SupervisorCNC,
    start_supervisor_cnc,
)
from enrgdaq.cnc.log_util import CNCLogHandler
from enrgdaq.cnc.models import SupervisorStatus
from enrgdaq.daq.alert.base import DAQJobAlert
from enrgdaq.daq.base import DAQJobProcess, _create_queue
from enrgdaq.daq.daq_job import (
    SUPERVISOR_CONFIG_FILE_NAME,
    load_daq_jobs,
    rebuild_daq_job,
    start_daq_job,
    start_daq_jobs,
)
from enrgdaq.daq.models import (
    DAQJobInfo,
    DAQJobMessageStop,
)
from enrgdaq.daq.store.base import DAQJobStore
from enrgdaq.daq.topics import Topic
from enrgdaq.message_broker import MessageBroker
from enrgdaq.models import (
    RestartScheduleInfo,
    SupervisorConfig,
    SupervisorInfo,
)
from enrgdaq.supervisor_message_handler import SupervisorMessageHandler
from enrgdaq.utils.network import get_available_port

DAQ_JOB_QUEUE_ACTION_TIMEOUT = 0.02
"""Time in seconds to wait for a DAQ job to process a message."""

DAQ_JOB_MARK_AS_ALIVE_TIME_SECONDS = 5
"""Time in seconds to mark a DAQ job as alive after it has been running for that long."""

DAQ_SUPERVISOR_STATS_MESSAGE_INTERVAL_SECONDS = 1
"""Time in seconds between sending supervisor stats messages."""

DAQ_SUPERVISOR_DEFAULT_RESTART_TIME_SECONDS = 2
"""Default time in seconds to wait before restarting a DAQ job."""

DAQ_SUPERVISOR_HEARTBEAT_MESSAGE_INTERVAL_SECONDS = 1
"""Time in seconds between sending supervisor heartbeat messages."""

DAQ_SUPERVISOR_ROUTES_MESSAGE_INTERVAL_SECONDS = 0.1
"""Time in seconds between sending supervisor routes messages."""


@dataclass
class RestartDAQJobSchedule:
    daq_job_process: DAQJobProcess
    restart_at: datetime


class Supervisor:
    """
    Supervisor class responsible for managing DAQ job threads, handling their lifecycle,
    and facilitating communication between them.
    Attributes:
        config (SupervisorConfig | None): Configuration for the supervisor.
        daq_job_processes (list[DAQJobProcess]): List of DAQ job processes managed by the supervisor.
        restart_schedules (list[RestartDAQJobSchedule]): List of schedules for restarting DAQ jobs.
        _logger (logging.Logger): Logger instance for logging supervisor activities.
        _last_stats_message_time (datetime): Last time a stats message was sent.
        _cnc_instance (SupervisorCNC | None): CNC instance for the supervisor.

        _last_stats_message_time (datetime): Last time a stats message was sent.
        _last_heartbeat_message_time (datetime): Last time a heartbeat message was sent.
        _log_queue (Queue[logging.LogRecord]): Queue for logging messages.
        _log_listener (QueueListener | None): Log listener for the supervisor.
        _is_stopped (bool): Whether the supervisor is stopped.
        _daq_jobs_to_load (list[DAQJobProcess] | None): List of DAQ jobs to load.
        _daq_job_config_path (str): Path to the DAQ job configuration file.
        _psutil_process_cache (dict[int, psutil.Process]): Cache for process information.
    """

    config: SupervisorConfig | None
    daq_job_processes: list[DAQJobProcess]
    restart_schedules: list[RestartDAQJobSchedule]
    _logger: logging.Logger
    _cnc_instance: SupervisorCNC | None = None

    _last_stats_message_time: datetime
    _last_heartbeat_message_time: datetime
    _last_routes_message_time: datetime
    _log_queue: Any
    _log_listener: QueueListener | None = None
    _is_stopped: bool
    _daq_jobs_to_load: list[DAQJobProcess] | None
    _daq_job_config_path: str
    _psutil_process_cache: dict[int, psutil.Process]

    def __init__(
        self,
        config: SupervisorConfig | None = None,
        daq_job_processes: list[DAQJobProcess] | None = None,
        daq_job_config_path: str = "configs/",
    ):
        self._daq_job_config_path = daq_job_config_path
        self._log_queue = _create_queue()
        self._log_listener = None
        self._logger = logging.getLogger()

        if config is None:
            self.config = self._load_supervisor_config()
        else:
            self.config = config

        self._daq_jobs_to_load = daq_job_processes
        self.daq_job_processes = []
        self.restart_schedules = []
        self.daq_job_remote_stats = {}
        self.daq_job_stats = {}
        self._psutil_process_cache = {}
        if not os.path.exists(self._daq_job_config_path):
            raise ValueError(
                f"DAQ job config path '{self._daq_job_config_path}' does not exist."
            )
        self._is_stopped = False

        self.message_broker = MessageBroker()
        random_id = str(uuid.uuid4())[:8]

        # IPC is not supported on Windows, use TCP with dynamic ports instead
        if sys.platform == "win32":
            xpub_port = get_available_port()
            xsub_port = get_available_port()
            self.supervisor_xpub_url = f"tcp://127.0.0.1:{xpub_port}"
            self.supervisor_xsub_url = f"tcp://127.0.0.1:{xsub_port}"
        else:
            self.supervisor_xpub_url = (
                f"ipc:///tmp/supervisor_{self.supervisor_id}_{random_id}_xpub.ipc"
            )
            self.supervisor_xsub_url = (
                f"ipc:///tmp/supervisor_{self.supervisor_id}_{random_id}_xsub.ipc"
            )

        self.message_broker.add_xpub_socket("supervisor_xpub", self.supervisor_xpub_url)
        self.message_broker.add_xsub_socket("supervisor_xsub", self.supervisor_xsub_url)
        self.message_broker.start_proxy(
            "supervisor_proxy", "supervisor_xpub", "supervisor_xsub"
        )

        self.message_handler = SupervisorMessageHandler(
            xpub_url=self.supervisor_xpub_url,
            supervisor_id=self.supervisor_id,
            on_stats_receive=lambda x: self._update_stats(x.stats),
            on_remote_stats_receive=lambda x: self._update_remote_stats(x.stats),
        )

        self._setup_federation()

        self._last_stats_message_time = datetime.min
        self._last_heartbeat_message_time = datetime.min
        self._last_routes_message_time = datetime.min

    def init(self):
        """
        Initializes the supervisor, loads configuration, starts DAQ job threads, and warns for lack of DAQ jobs.

        You should call this method after creating a new instance of the Supervisor class.
        """

        self._logger.setLevel(
            self.config.verbosity.to_logging_level() if self.config else logging.INFO
        )

        assert self.config is not None

        # Change logging name based on supervisor id
        self._logger.name = f"Supervisor({self.config.info.supervisor_id})"

        if self.config.cnc is not None:
            self._logger.debug("Starting CNC instance...")
            self._cnc_instance = start_supervisor_cnc(
                supervisor=self,
                config=self.config.cnc,
            )

        # Initialize ring buffer for zero-copy PyArrow message transfer
        if sys.platform != "win32":
            from enrgdaq.utils.shared_ring_buffer import get_global_ring_buffer

            try:
                # get hash of supervisor_id to fit name
                assert self.config is not None

                supervisor_id_hash = base64.b64encode(
                    hashlib.md5(self.config.info.supervisor_id.encode()).digest()
                ).decode()
                get_global_ring_buffer(
                    name=f"ring_{supervisor_id_hash}",
                    total_size=self.config.ring_buffer_size_mb * 1024 * 1024,
                    slot_size=self.config.ring_buffer_slot_size_kb * 1024,
                )
                self._logger.info(
                    f"Initialized ring buffer: {self.config.ring_buffer_size_mb}MB total, "
                    f"{self.config.ring_buffer_slot_size_kb}KB per slot"
                )
            except Exception as e:
                self._logger.warning(f"Failed to initialize ring buffer: {e}")

        # Set up log listener to capture logs from child processes
        handlers: list[logging.Handler] = []
        if self._cnc_instance:
            # Find the CNCLogHandler in the CNC instance logger
            cnc_handler = next(
                (
                    h
                    for h in self._cnc_instance._logger.handlers
                    if isinstance(h, CNCLogHandler)
                ),
                None,
            )
            if cnc_handler:
                handlers.append(cnc_handler)
            else:
                # Fallback to root handlers if CNC handler is missing
                handlers.extend(logging.getLogger().handlers)
        else:
            handlers.extend(logging.getLogger().handlers)

        if handlers:
            self._log_listener = QueueListener(self._log_queue, *handlers)
            self._log_listener.start()

        self.restart_schedules = []
        self.daq_job_processes = []
        self._logger.debug("Starting DAQ job processes...")
        self.start_daq_job_processes(self._daq_jobs_to_load or [])
        self._logger.debug(f"Started {len(self.daq_job_processes)} DAQ job processes")
        self.warn_for_lack_of_daq_jobs()

        self.message_handler.start()

        self._last_stats_message_time = datetime.min
        self._last_heartbeat_message_time = datetime.min

    def start_daq_job_processes(self, daq_jobs_to_load: list[DAQJobProcess]):
        assert self.config is not None

        # Start threads from user-provided daq jobs, or by
        # reading the config files like usual
        jobs_to_start = daq_jobs_to_load or load_daq_jobs(
            self._daq_job_config_path, self.config.info
        )

        for job in jobs_to_start:
            job.log_queue = self._log_queue
            job.zmq_xpub_url = self.supervisor_xpub_url
            job.zmq_xsub_url = self.supervisor_xsub_url

        started_jobs = start_daq_jobs(jobs_to_start)
        self.daq_job_processes.extend(started_jobs)

    def run(self):
        """
        Main loop that continuously runs the supervisor, handling job restarts and message passing.
        """
        while not self._is_stopped:
            try:
                self.loop()
            except KeyboardInterrupt:
                self._logger.warning("KeyboardInterrupt received, stopping")
                self.stop()
                break
        self.message_broker.send(
            DAQJobMessageStop(
                reason="Stopped by supervisor",
                topics={Topic.supervisor_broadcast(self.supervisor_id)},
            )
        )

    def stop(self):
        """
        Stops the supervisor and all its components.
        """
        if self._is_stopped:
            return
        if self._cnc_instance:
            self._cnc_instance.stop()
        if self._log_listener:
            self._log_listener.stop()

        # Clean up ring buffer
        if sys.platform != "win32":
            from enrgdaq.utils.shared_ring_buffer import cleanup_global_ring_buffer

            try:
                cleanup_global_ring_buffer()
                self._logger.debug("Ring buffer cleaned up")
            except Exception as e:
                self._logger.warning(f"Failed to cleanup ring buffer: {e}")

        self._is_stopped = True

    def loop(self):
        """
        A single iteration of the supervisor's main loop.
        """

        # Remove dead threads
        dead_processes = [
            t for t in self.daq_job_processes if t.process and not t.process.is_alive()
        ]
        # Clean up dead threads
        self.daq_job_processes = [
            t for t in self.daq_job_processes if t not in dead_processes
        ]

        # Get restart schedules for dead jobs
        self.restart_schedules.extend(self.get_restart_schedules(dead_processes))

        # Restart jobs that have stopped or are scheduled to restart
        self.restart_daq_jobs()

    def get_status(self) -> SupervisorStatus:
        """
        Gets the status of the supervisor and its DAQ jobs.

        Returns:
            SupervisorStatus: A struct containing the status.
        """
        assert self.config is not None
        return SupervisorStatus(
            daq_jobs=[
                daq_job_process.daq_job_info
                for daq_job_process in self.daq_job_processes
                if daq_job_process.daq_job_info is not None
            ],
            supervisor_info=self.config.info,
            daq_job_stats=self.daq_job_stats,
            daq_job_remote_stats=self.daq_job_remote_stats,
            restart_schedules=[
                RestartScheduleInfo(
                    job=sched.daq_job_process.daq_job_cls.__name__,
                    restart_at=sched.restart_at.isoformat(),
                )
                for sched in self.restart_schedules
            ],
        )

    def get_restart_schedules(
        self, dead_processes: list[DAQJobProcess]
    ) -> list[RestartDAQJobSchedule]:
        """
        Gets the restart schedules for the dead threads.

        Args:
            dead_processes (list[DAQJobProcess]): List of dead processes.

        Returns:
            list[RestartDAQJobSchedule]: List of restart schedules.
        """

        res: list[RestartDAQJobSchedule] = []
        for process in dead_processes:
            # Skip processes that should not be restarted on crash
            if not process.restart_on_crash:
                self._logger.info(
                    f"Removing {process.daq_job_cls.__name__} from process list"
                )
                if process in self.daq_job_processes:
                    self.daq_job_processes.remove(process)
                continue

            restart_offset = getattr(process.daq_job_cls, "restart_offset", None)
            if not isinstance(restart_offset, timedelta):
                restart_offset = timedelta(
                    seconds=DAQ_SUPERVISOR_DEFAULT_RESTART_TIME_SECONDS
                )
            self._logger.info(
                f"Scheduling restart of {process.daq_job_cls.__name__} in {restart_offset.total_seconds()} seconds"
            )
            res.append(
                RestartDAQJobSchedule(
                    daq_job_process=process,
                    restart_at=datetime.now() + restart_offset,
                )
            )
        return res

    def restart_daq_jobs(self):
        """
        Restarts the DAQ jobs that have been scheduled for restart.
        """
        assert self.config is not None

        schedules_to_remove: list[RestartDAQJobSchedule] = []
        for restart_schedule in self.restart_schedules:
            if datetime.now() < restart_schedule.restart_at:
                continue
            new_daq_job_process = rebuild_daq_job(
                restart_schedule.daq_job_process, self.config.info
            )
            # Set ZMQ URLs and log queue for the restarted process
            new_daq_job_process.log_queue = self._log_queue
            new_daq_job_process.zmq_xpub_url = self.supervisor_xpub_url
            new_daq_job_process.zmq_xsub_url = self.supervisor_xsub_url

            self.daq_job_processes.append(start_daq_job(new_daq_job_process))

            schedules_to_remove.append(restart_schedule)

        # Remove processed schedules
        self.restart_schedules = [
            x for x in self.restart_schedules if x not in schedules_to_remove
        ]

    def warn_for_lack_of_daq_jobs(self):
        DAQ_JOB_ABSENT_WARNINGS = {
            DAQJobStore: "No store job found, data will not be stored",
            DAQJobAlert: "No alert job found, alerts will not be sent",
        }

        for daq_job_type, warning_message in DAQ_JOB_ABSENT_WARNINGS.items():
            if not any(
                process
                for process in self.daq_job_processes
                if issubclass(process.daq_job_cls, daq_job_type)
            ):
                self._logger.warning(warning_message)

    def _load_supervisor_config(self):
        supervisor_config_file_path = os.path.join(
            self._daq_job_config_path, SUPERVISOR_CONFIG_FILE_NAME
        )
        if not os.path.exists(supervisor_config_file_path):
            self._logger.warning(
                f"No supervisor config file found at '{supervisor_config_file_path}', using default config"
            )
            return SupervisorConfig(info=SupervisorInfo(supervisor_id=platform.node()))

        with open(supervisor_config_file_path, "rb") as f:
            return msgspec.toml.decode(f.read(), type=SupervisorConfig)

    @cache
    def _get_supervisor_daq_job_info(self):
        assert self.config is not None
        return DAQJobInfo(
            daq_job_type="Supervisor",
            supervisor_info=self.config.info,
            unique_id=self.config.info.supervisor_id,
            instance_id=0,
            config="",
        )

    def _setup_federation(self) -> None:
        """
        Set up federation between supervisors in a star topology.

        If this supervisor is the server:
            - Exposes additional XPUB/XSUB endpoints for clients to connect to
            - Starts a proxy between these endpoints

        If this supervisor is a client:
            - Connects to the server's XPUB to receive messages
            - Connects to the server's XSUB to send messages
            - Starts forwarder threads for bidirectional communication
        """
        if self.config is None or self.config.federation is None:
            return

        fed = self.config.federation

        if fed.is_server:
            # Server mode: add federation binds to existing supervisor sockets
            # This allows remote clients to connect to the same proxy as local jobs
            if fed.server_xpub_url and fed.server_xsub_url:
                self._logger.info(
                    f"Adding federation endpoints: XPUB={fed.server_xpub_url}, XSUB={fed.server_xsub_url}"
                )
                # Bind additional addresses to existing sockets
                xpub_socket = self.message_broker.xpub_sockets["supervisor_xpub"]
                xsub_socket = self.message_broker.xsub_sockets["supervisor_xsub"]
                xpub_socket.bind(fed.server_xpub_url)
                xsub_socket.bind(fed.server_xsub_url)
                self._logger.info("Federation server ready")
            else:
                self._logger.warning(
                    "Federation server mode enabled but XPUB/XSUB URLs not configured"
                )
        else:
            # Client mode: connect to remote server
            if fed.remote_server_xpub_url and fed.remote_server_xsub_url:
                self._logger.info(
                    f"Connecting to federation server XSUB={fed.remote_server_xsub_url}"
                )

                # Create PUB socket to send messages to server
                self._fed_pub_socket = self.message_broker.connect_pub_to_xsub(
                    "federation_pub", fed.remote_server_xsub_url
                )

                # Start forwarder thread (local -> server only)
                self._start_federation_forwarders()

                self._logger.info("Connected to federation server")
            else:
                self._logger.warning(
                    "Federation client mode but remote server URLs not configured"
                )

    def _start_federation_forwarders(self) -> None:
        """
        Start forwarder to push messages from local proxy to federation server.

        Uses ZMQ Poller for efficient message forwarding while being stoppable.

        Note: We only forward LOCAL → SERVER, not the reverse.
        If we forwarded server messages back to local, it would create a loop:
        (local → server → back to local → server again → ...)
        """

        # Create SUB socket to receive from local XPUB
        local_sub = self.message_broker.context.socket(zmq.SUB)
        local_sub.connect(self.supervisor_xpub_url)
        local_sub.setsockopt_string(zmq.SUBSCRIBE, "")

        def forward_to_server():
            self._logger.debug("Federation forwarder (local -> server) started")
            poller = zmq.Poller()
            poller.register(local_sub, zmq.POLLIN)
            try:
                while not self._is_stopped:
                    events = dict(poller.poll(100))  # 100ms timeout
                    if local_sub in events:
                        msg = local_sub.recv_multipart(zmq.NOBLOCK)
                        self._fed_pub_socket.send_multipart(msg)
            except zmq.ContextTerminated:
                pass
            finally:
                local_sub.close()
                self._logger.debug("Federation forwarder (local -> server) stopped")

        self._fed_to_server_thread = threading.Thread(
            target=forward_to_server, daemon=True
        )
        self._fed_to_server_thread.start()

    def _update_stats(self, stats):
        self.daq_job_stats = stats

    def _update_remote_stats(self, stats):
        self.daq_job_remote_stats = stats

    @property
    def supervisor_id(self):
        if self.config is None or self.config.info is None:
            return "unknown"
        return self.config.info.supervisor_id
