import logging
import os
import platform
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from queue import Empty, Full
from typing import Optional

import msgspec

from enrgdaq.cnc.base import (
    SupervisorCNC,
    start_supervisor_cnc,
)
from enrgdaq.cnc.models import SupervisorStatus
from enrgdaq.daq.alert.base import DAQJobAlert
from enrgdaq.daq.base import DAQJob, DAQJobProcess
from enrgdaq.daq.daq_job import (
    SUPERVISOR_CONFIG_FILE_NAME,
    load_daq_jobs,
    rebuild_daq_job,
    start_daq_job,
    start_daq_jobs,
)
from enrgdaq.daq.jobs.handle_stats import DAQJobMessageStats, DAQJobStatsDict
from enrgdaq.daq.jobs.remote import DAQJobMessageStatsRemote, DAQJobRemoteStatsDict
from enrgdaq.daq.models import (
    DAQJobInfo,
    DAQJobMessage,
    DAQJobMessageStop,
    DAQJobStats,
)
from enrgdaq.daq.store.base import DAQJobStore
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


@dataclass
class RestartDAQJobSchedule:
    daq_job_process: DAQJobProcess
    restart_at: datetime


class Supervisor:
    """
    Supervisor class responsible for managing DAQ job threads, handling their lifecycle,
    and facilitating communication between them.
    Attributes:
        daq_job_threads (list[DAQJobThread]): List of DAQ job threads managed by the supervisor.
        daq_job_stats (DAQJobStatsDict): Dictionary holding statistics for each DAQ job type.
        restart_schedules (list[RestartDAQJobSchedule]): List of schedules for restarting DAQ jobs.
        _logger (logging.Logger): Logger instance for logging supervisor activities.
        _last_stats_message_time (datetime): Last time a stats message was sent.
    """

    daq_job_processes: list[DAQJobProcess]
    daq_job_stats: DAQJobStatsDict
    daq_job_remote_stats: DAQJobRemoteStatsDict
    restart_schedules: list[RestartDAQJobSchedule]
    _logger: logging.Logger
    _cnc_instance: Optional[SupervisorCNC] = None

    _last_stats_message_time: datetime
    _last_heartbeat_message_time: datetime

    def __init__(
        self,
        config: Optional[SupervisorConfig] = None,
        daq_job_processes: Optional[list[DAQJobProcess]] = None,
        daq_job_config_path: str = "configs/",
    ):
        self.config = config
        self._daq_jobs_to_load = daq_job_processes
        self.daq_job_remote_stats = {}
        self._daq_job_config_path = daq_job_config_path
        if not os.path.exists(self._daq_job_config_path):
            raise ValueError(
                f"DAQ job config path '{self._daq_job_config_path}' does not exist."
            )
        pass

    def init(self):
        """
        Initializes the supervisor, loads configuration, starts DAQ job threads, and warns for lack of DAQ jobs.

        You should call this method after creating a new instance of the Supervisor class.
        """

        self._logger = logging.getLogger()
        if not self.config:
            self.config = self._load_supervisor_config()

        # Change logging name based on supervisor id
        self._logger.name = f"Supervisor({self.config.info.supervisor_id})"

        if self.config.cnc is not None:
            self._cnc_instance = start_supervisor_cnc(
                supervisor=self,
                config=self.config.cnc,
            )

        self.restart_schedules = []
        self.daq_job_processes = []
        self.start_daq_job_processes(self._daq_jobs_to_load or [])
        self.daq_job_stats: DAQJobStatsDict = {
            thread.daq_job_cls.__name__: DAQJobStats()
            for thread in self.daq_job_processes
        }
        self.warn_for_lack_of_daq_jobs()

        self._last_stats_message_time = datetime.min
        self._last_heartbeat_message_time = datetime.min

    def start_daq_job_processes(self, daq_jobs_to_load: list[DAQJobProcess]):
        assert self.config is not None
        # Start threads from user-provided daq jobs, or by
        # reading the config files like usual
        started_jobs = start_daq_jobs(
            daq_jobs_to_load
            or load_daq_jobs(self._daq_job_config_path, self.config.info)
        )
        self.daq_job_processes.extend(started_jobs)

    def run(self):
        """
        Main loop that continuously runs the supervisor, handling job restarts and message passing.
        """
        while True:
            try:
                self.loop()
            except KeyboardInterrupt:
                self._logger.warning("KeyboardInterrupt received, cleaning up")
                self.stop()
                break

    def stop(self):
        """
        Stops the supervisor and all its components.
        """
        if self._cnc_instance:
            self._cnc_instance.stop()
        for daq_job_process in self.daq_job_processes:
            daq_job_process.message_out.put(
                DAQJobMessageStop(reason="KeyboardInterrupt")
            )
        # Wait for all threads to stop with a timeout
        for daq_job_process in self.daq_job_processes:
            if not daq_job_process.process:
                continue
            daq_job_process.process.join(timeout=5)

        sys.exit(0)

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

    def get_restart_schedules(
        self, dead_processes: list[DAQJobProcess]
    ) -> list[RestartDAQJobSchedule]:
        """
        Gets the restart schedules for the dead threads.

        Args:
            dead_threads (list[DAQJobThread]): List of dead threads.

        Returns:
            list[RestartDAQJobSchedule]: List of restart schedules.
        """

        res = []
        for process in dead_processes:
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

        schedules_to_remove = []
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

        messages = []

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
        # Send heartbeat message, targeted at the remote
        """
        if datetime.now() > self._last_heartbeat_message_time + timedelta(
            seconds=DAQ_SUPERVISOR_HEARTBEAT_MESSAGE_INTERVAL_SECONDS
        ):
            self._last_heartbeat_message_time = datetime.now()
            messages.append(
                DAQJobMessageHeartbeat(
                    daq_job_info=self._get_supervisor_daq_job_info(),
                )
            )
        """
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
        res = []
        for process in self.daq_job_processes:
            try:
                while True:
                    msg = process.message_out.get_nowait()
                    if msg.daq_job_info is None:
                        self._logger.warning(f"Message {msg} has no daq_job_info")
                        # msg.daq_job_info = process.daq_job.info
                    res.append(msg)
                    # Catch remote stats message
                    # TODO: These should be in a different class
                    if (
                        isinstance(msg, DAQJobMessageStatsRemote)
                        and msg.supervisor_id == self.config.info.supervisor_id
                    ):
                        self.daq_job_remote_stats = msg.stats
                    # Update stats
                    self.get_daq_job_stats(
                        self.daq_job_stats, process.daq_job_cls
                    ).message_out_stats.increase()
            except Empty:
                pass
        return res

    def send_messages_to_daq_jobs(self, daq_messages: list[DAQJobMessage]):
        """
        Sends messages to the DAQ jobs.

        Args:
            daq_messages (list[DAQJobMessage]): List of messages to send.
        """

        for message in daq_messages:
            for process in self.daq_job_processes:
                daq_job_cls = process.daq_job_cls
                # Send if message is allowed for this DAQ Job
                if not any(
                    isinstance(message, msg_type)
                    for msg_type in daq_job_cls.allowed_message_in_types
                ):
                    continue

                # Check if message is allowed for store
                # TODO: Maybe a better way to do this?
                if (
                    isinstance(daq_job_cls, type)
                    and getattr(daq_job_cls, "can_handle_message", None)
                    and not daq_job_cls.can_handle_message(message)  # type: ignore
                ):
                    continue

                # Send message
                try:
                    process.message_in.put_nowait(
                        message  # , timeout=DAQ_JOB_QUEUE_ACTION_TIMEOUT
                    )
                except Full:
                    self._logger.warning(
                        f"Message queue for {process.daq_job_cls.__name__} is full, dropping message"
                    )
                    continue

                # Update stats
                stats = self.get_daq_job_stats(self.daq_job_stats, daq_job_cls)
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

    def _get_supervisor_daq_job_info(self):
        assert self.config is not None
        return DAQJobInfo(
            daq_job_type="Supervisor",
            daq_job_class_name="Supervisor",
            supervisor_info=self.config.info,
            unique_id=self.config.info.supervisor_id,
            instance_id=0,
        )
