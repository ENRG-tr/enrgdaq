import base64
import logging
import os
import platform
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cache
from logging.handlers import QueueListener
from queue import Empty, Full
from typing import Any

import msgspec
import psutil

from enrgdaq.cnc.base import (
    SupervisorCNC,
    start_supervisor_cnc,
)
from enrgdaq.cnc.log_util import CNCLogHandler
from enrgdaq.cnc.models import SupervisorStatus
from enrgdaq.daq.alert.base import DAQJobAlert
from enrgdaq.daq.base import DAQJob, DAQJobProcess, _create_queue
from enrgdaq.daq.daq_job import (
    SUPERVISOR_CONFIG_FILE_NAME,
    load_daq_jobs,
    rebuild_daq_job,
    start_daq_job,
    start_daq_jobs,
)
from enrgdaq.daq.jobs.handle_stats import DAQJobMessageStats, DAQJobStatsDict
from enrgdaq.daq.jobs.remote import (
    DAQJobMessageStatsRemote,
    DAQJobRemote,
    DAQJobRemoteStatsDict,
)
from enrgdaq.daq.models import (
    DAQJobInfo,
    DAQJobMessage,
    DAQJobMessageSHM,
    DAQJobMessageStatsReport,
    DAQJobMessageStop,
    DAQJobStats,
    InternalDAQJobMessage,
    RouteMapping,
)
from enrgdaq.daq.store.base import DAQJobStore
from enrgdaq.daq.store.models import DAQJobMessageStoreSHM
from enrgdaq.message_broker import MessageBroker
from enrgdaq.models import (
    RestartScheduleInfo,
    SupervisorConfig,
    SupervisorInfo,
)

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
        daq_job_stats (DAQJobStatsDict): Dictionary holding statistics for each DAQ job type.
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
    """

    config: SupervisorConfig | None
    daq_job_processes: list[DAQJobProcess]
    daq_job_stats: DAQJobStatsDict
    daq_job_remote_stats: DAQJobRemoteStatsDict
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
        self.config = config
        self._daq_jobs_to_load = daq_job_processes
        self.daq_job_processes = []
        self.restart_schedules = []
        self.daq_job_remote_stats = {}
        self.daq_job_stats = {}
        self._daq_job_config_path = daq_job_config_path
        self._psutil_process_cache = {}
        if not os.path.exists(self._daq_job_config_path):
            raise ValueError(
                f"DAQ job config path '{self._daq_job_config_path}' does not exist."
            )
        self._is_stopped = False

        self._log_queue = _create_queue()
        self._log_listener = None
        self._logger = logging.getLogger()

        self.message_broker = MessageBroker()
        self.supervisor_xpub_url = (
            f"ipc:///tmp/supervisor_{self.supervisor_id}_xpub.ipc"
        )
        self.supervisor_xsub_url = (
            f"ipc:///tmp/supervisor_{self.supervisor_id}_xsub.ipc"
        )

        self.message_broker.add_xpub_socket("supervisor_xpub", self.supervisor_xpub_url)
        self.message_broker.add_xsub_socket("supervisor_xsub", self.supervisor_xsub_url)
        self.message_broker.start_proxy(
            "supervisor_proxy", "supervisor_xpub", "supervisor_xsub"
        )

        self._last_stats_message_time = datetime.min
        self._last_heartbeat_message_time = datetime.min
        self._last_routes_message_time = datetime.min

    def init(self):
        """
        Initializes the supervisor, loads configuration, starts DAQ job threads, and warns for lack of DAQ jobs.

        You should call this method after creating a new instance of the Supervisor class.
        """

        if not self.config:
            self.config = self._load_supervisor_config()

        self._logger.setLevel(
            self.config.verbosity.to_logging_level() if self.config else logging.INFO
        )

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
                import hashlib

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
        self.daq_job_stats.update(
            {
                thread.daq_job_cls.__name__: DAQJobStats()
                for thread in self.daq_job_processes
            }
        )
        self.warn_for_lack_of_daq_jobs()

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
        for daq_job_process in self.daq_job_processes:
            try:
                daq_job_process.message_out.put_nowait(
                    DAQJobMessageStop(reason="Stopped by supervisor")
                )
            except Exception:
                # Queue might be closed, continue
                pass

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

        # Handle process alive stats for dead & alive processes
        self.handle_process_alive_stats(dead_processes)

        # Get messages from DAQ Jobs
        daq_messages_out = self.get_messages_from_daq_jobs()

        # Add supervisor messages
        daq_messages_out.extend(self.get_supervisor_messages())

        # Send messages to appropriate DAQ Jobs
        self.send_messages_to_daq_jobs(daq_messages_out)

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

    def handle_process_alive_stats(self, dead_processes: list[DAQJobProcess]):
        """
        Handles the alive stats for the dead threads.

        Args:
            dead_threads (list[DAQJobThread]): List of dead threads.
        """

        for process in self.daq_job_processes:
            if datetime.now() - process.start_time > timedelta(
                seconds=DAQ_JOB_MARK_AS_ALIVE_TIME_SECONDS
            ):
                self.get_daq_job_stats(
                    self.daq_job_stats, process.daq_job_cls
                ).is_alive = True

        for process in dead_processes:
            self.get_daq_job_stats(
                self.daq_job_stats, process.daq_job_cls
            ).is_alive = False

        # Resource Monitoring
        for process in self.daq_job_processes:
            if process.process and process.process.is_alive():
                try:
                    pid = process.process.pid
                    if pid is None:
                        continue
                    if pid not in self._psutil_process_cache:
                        self._psutil_process_cache[pid] = psutil.Process(pid)

                    p = self._psutil_process_cache[pid]
                    stats = self.get_daq_job_stats(
                        self.daq_job_stats, process.daq_job_cls
                    )
                    # Use cpu_percent() - first call usually 0.0,
                    # but since we cache the process object, subsequent calls will be accurate.
                    stats.resource_stats.cpu_percent = p.cpu_percent()
                    stats.resource_stats.rss_mb = p.memory_info().rss / 1024 / 1024
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    if process.process.pid in self._psutil_process_cache:
                        del self._psutil_process_cache[process.process.pid]
                    pass

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
            self.daq_job_processes.append(start_daq_job(new_daq_job_process))

            # Update restart stats
            self.get_daq_job_stats(
                self.daq_job_stats, new_daq_job_process.daq_job_cls
            ).restart_stats.increase()
            schedules_to_remove.append(restart_schedule)

        # Remove processed schedules
        self.restart_schedules = [
            x for x in self.restart_schedules if x not in schedules_to_remove
        ]

    def get_supervisor_messages(self) -> list[DAQJobMessage]:
        """
        Gets the supervisor messages to be sent to the DAQ jobs.

        Returns:
            list[DAQJobMessage]: List of supervisor messages.
        """

        messages: list[DAQJobMessage] = []

        # Send stats message
        if datetime.now() > self._last_stats_message_time + timedelta(
            seconds=DAQ_SUPERVISOR_STATS_MESSAGE_INTERVAL_SECONDS
        ):
            self._last_stats_message_time = datetime.now()
            messages.append(
                DAQJobMessageStats(
                    stats=self.daq_job_stats,
                    daq_job_info=self._get_supervisor_daq_job_info(),
                )
            )

        return messages

    def get_daq_job_stats(
        self, daq_job_stats: DAQJobStatsDict, daq_job_type: type[DAQJob]
    ) -> DAQJobStats:
        daq_job_type_name = daq_job_type.__name__
        if daq_job_type_name not in daq_job_stats:
            daq_job_stats[daq_job_type_name] = DAQJobStats()
        return daq_job_stats[daq_job_type_name]

    def get_messages_from_daq_jobs(self) -> list[DAQJobMessage]:
        assert self.config is not None
        res: list[DAQJobMessage] = []
        for process in self.daq_job_processes:
            try:
                while True:
                    break
                    msg = process.message_out.get_nowait()
                    if msg.daq_job_info is None:
                        self._logger.warning(f"Message {msg} has no daq_job_info")
                        # msg.daq_job_info = process.daq_job.info
                    res.append(msg)
                    if (
                        isinstance(msg, DAQJobMessageStatsRemote)
                        and msg.supervisor_id == self.config.info.supervisor_id
                    ):
                        self.daq_job_remote_stats = msg.stats

                    if isinstance(msg, DAQJobMessageStatsReport):
                        stats = self.get_daq_job_stats(
                            self.daq_job_stats, process.daq_job_cls
                        )
                        stats.latency_stats = msg.latency
                        stats.message_in_stats.set(msg.processed_count)
                        stats.message_out_stats.set(msg.sent_count)

                    # Update stats
                    self.get_daq_job_stats(
                        self.daq_job_stats, process.daq_job_cls
                    ).message_out_stats.increase()
            except Empty:
                pass

        if len(res) > 0:
            self._logger.debug(f"Got {len(res)} messages from DAQ jobs: {res}")
        return res

    def send_messages_to_daq_jobs(self, daq_messages: list[DAQJobMessage]):
        """
        Sends messages to the DAQ jobs.

        Args:
            daq_messages (list[DAQJobMessage]): List of messages to send.
        """

        for message in daq_messages:
            for process in self.daq_job_processes:
                break
                # Do not send to the same DAQ job
                if (
                    message.daq_job_info
                    and process.daq_job_info
                    and message.daq_job_info.unique_id == process.daq_job_info.unique_id
                ):
                    continue

                daq_job_cls = process.daq_job_cls
                # Send if message is allowed for this DAQ Job
                if not any(
                    isinstance(message, msg_type)
                    for msg_type in daq_job_cls.allowed_message_in_types
                ) and not isinstance(message, InternalDAQJobMessage):
                    continue

                # Check if base class can handle such message
                if (
                    getattr(daq_job_cls, "can_handle_message", None)
                    and not daq_job_cls.can_handle_message(message)  # type: ignore
                    and not isinstance(daq_job_cls, InternalDAQJobMessage)
                ):
                    continue

                # Debug: Log when sending to DAQJobRemote
                if daq_job_cls.__name__ == "DAQJobRemote":
                    self._logger.debug(
                        f"Sending {type(message).__name__} to DAQJobRemote"
                    )

                # Send message
                try:
                    process.message_in.put_nowait(message)
                except Full:
                    # Clean SHM
                    if isinstance(message, DAQJobMessageSHM) or isinstance(
                        message, DAQJobMessageStoreSHM
                    ):
                        message.shm.cleanup()
                    continue

                # Update stats
                stats = self.get_daq_job_stats(
                    self.daq_job_stats,
                    daq_job_cls,  # type: ignore
                )
                stats.message_in_stats.increase()

                # Do not update stats if Mac OS X, as it does not support queue.qsize()
                if sys.platform != "darwin":
                    stats.message_in_queue_stats.set(process.message_in.qsize())
                    stats.message_out_queue_stats.set(process.message_out.qsize())

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

    def _generate_route_mapping(self) -> RouteMapping:
        routes: RouteMapping = defaultdict(list)
        for process in self.daq_job_processes:
            if issubclass(process.daq_job_cls, DAQJobStore) or issubclass(
                process.daq_job_cls, DAQJobRemote
            ):
                break
                routes[process.daq_job_cls.__name__].append(process.message_in)

        return dict(routes)

    @property
    def supervisor_id(self):
        if self.config is None or self.config.info is None:
            return "unknown"
        return self.config.info.supervisor_id
