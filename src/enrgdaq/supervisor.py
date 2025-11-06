import logging
import os
import platform
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from queue import Empty
from typing import Optional

import msgspec

from enrgdaq.daq.alert.base import DAQJobAlert
from enrgdaq.daq.base import DAQJob, DAQJobProcess
from enrgdaq.daq.daq_job import (
    SUPERVISOR_CONFIG_FILE_PATH,
    load_daq_jobs,
    restart_daq_job,
    start_daq_jobs,
)
from enrgdaq.daq.jobs.handle_stats import DAQJobMessageStats, DAQJobStatsDict
from enrgdaq.daq.jobs.remote import DAQJobMessageStatsRemote, DAQJobRemoteStatsDict
from enrgdaq.daq.models import DAQJobConfig, DAQJobInfo, DAQJobMessage, DAQJobStats
from enrgdaq.daq.store.base import DAQJobStore
from enrgdaq.models import SupervisorConfig

DAQ_JOB_QUEUE_ACTION_TIMEOUT = 0.02
"""Time in seconds to wait for a DAQ job to process a message."""

DAQ_JOB_MARK_AS_ALIVE_TIME_SECONDS = 5
"""Time in seconds to mark a DAQ job as alive after it has been running for that long."""

DAQ_SUPERVISOR_STATS_MESSAGE_INTERVAL_SECONDS = 1
"""Time in seconds between sending supervisor stats messages."""


@dataclass
class RestartDAQJobSchedule:
    daq_job_type: type[DAQJob]
    daq_job_config: DAQJobConfig
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

    _last_stats_message_time: datetime

    def __init__(
        self,
        config: Optional[SupervisorConfig] = None,
        daq_jobs: Optional[list[DAQJob]] = None,
    ):
        self.config = config
        self._daq_jobs_to_load = daq_jobs
        self.daq_job_remote_stats = {}
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
        self._logger.name = f"Supervisor({self.config.supervisor_id})"

        self.restart_schedules = []
        self.daq_job_processes = self.start_daq_job_processes(self._daq_jobs_to_load)
        self.daq_job_stats: DAQJobStatsDict = {
            type(thread.daq_job): DAQJobStats() for thread in self.daq_job_processes
        }
        self.warn_for_lack_of_daq_jobs()

        self._last_stats_message_time = datetime.now() - timedelta(
            seconds=DAQ_SUPERVISOR_STATS_MESSAGE_INTERVAL_SECONDS
        )

    def start_daq_job_processes(
        self, daq_jobs_to_load: Optional[list[DAQJob]] = None
    ) -> list[DAQJobProcess]:
        assert self.config is not None
        # Start threads from user-provided daq jobs, or by
        # reading the config files like usual
        return start_daq_jobs(
            daq_jobs_to_load or load_daq_jobs("configs/", self.config)
        )

    def run(self):
        """
        Main loop that continuously runs the supervisor, handling job restarts and message passing.
        """
        while True:
            try:
                self.loop()
            except KeyboardInterrupt:
                self._logger.warning("KeyboardInterrupt received, cleaning up")
                for daq_job_thread in self.daq_job_processes:
                    daq_job_thread.daq_job.__del__()
                break

    def loop(self):
        """
        A single iteration of the supervisor's main loop.
        """

        # Remove dead threads
        dead_threads = [t for t in self.daq_job_processes if not t.thread.is_alive()]
        # Clean up dead threads
        self.daq_job_processes = [
            t for t in self.daq_job_processes if t not in dead_threads
        ]

        # Get restart schedules for dead jobs
        self.restart_schedules.extend(self.get_restart_schedules(dead_threads))

        # Restart jobs that have stopped or are scheduled to restart
        self.restart_daq_jobs()

        # Handle thread alive stats for dead & alive threads
        self.handle_thread_alive_stats(dead_threads)

        # Get messages from DAQ Jobs
        daq_messages_out = self.get_messages_from_daq_jobs()

        # Add supervisor messages
        daq_messages_out.extend(self.get_supervisor_messages())

        # Send messages to appropriate DAQ Jobs
        self.send_messages_to_daq_jobs(daq_messages_out)

    def handle_thread_alive_stats(self, dead_threads: list[DAQJobProcess]):
        """
        Handles the alive stats for the dead threads.

        Args:
            dead_threads (list[DAQJobThread]): List of dead threads.
        """

        for thread in self.daq_job_processes:
            if datetime.now() - thread.start_time > timedelta(
                seconds=DAQ_JOB_MARK_AS_ALIVE_TIME_SECONDS
            ):
                self.get_daq_job_stats(
                    self.daq_job_stats, type(thread.daq_job)
                ).is_alive = True

        for thread in dead_threads:
            self.get_daq_job_stats(
                self.daq_job_stats, type(thread.daq_job)
            ).is_alive = False

    def get_restart_schedules(self, dead_processes: list[DAQJobProcess]):
        """
        Gets the restart schedules for the dead threads.

        Args:
            dead_threads (list[DAQJobThread]): List of dead threads.

        Returns:
            list[RestartDAQJobSchedule]: List of restart schedules.
        """

        res = []
        for process in dead_processes:
            restart_offset = getattr(process.daq_job, "restart_offset", None)
            if not isinstance(restart_offset, timedelta):
                restart_offset = timedelta(seconds=0)
            else:
                self._logger.info(
                    f"Scheduling restart of {type(process.daq_job).__name__} in {restart_offset.total_seconds()} seconds"
                )
            res.append(
                RestartDAQJobSchedule(
                    daq_job_type=type(process.daq_job),
                    daq_job_config=process.daq_job.config,
                    restart_at=datetime.now() + restart_offset,
                )
            )
            process.daq_job.free()
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
            self.daq_job_processes.append(
                restart_daq_job(
                    restart_schedule.daq_job_type,
                    restart_schedule.daq_job_config,
                    self.config,
                )
            )

            # Update restart stats
            self.get_daq_job_stats(
                self.daq_job_stats, restart_schedule.daq_job_type
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
        return messages

    def get_daq_job_stats(
        self, daq_job_stats: DAQJobStatsDict, daq_job_type: type[DAQJob]
    ) -> DAQJobStats:
        if daq_job_type not in daq_job_stats:
            daq_job_stats[daq_job_type] = DAQJobStats()
        return daq_job_stats[daq_job_type]

    def get_messages_from_daq_jobs(self) -> list[DAQJobMessage]:
        assert self.config is not None
        res = []
        for thread in self.daq_job_processes:
            try:
                while True:
                    msg = thread.daq_job.message_out.get_nowait()
                    if msg.daq_job_info is None:
                        msg.daq_job_info = thread.daq_job.info
                    res.append(msg)
                    # Catch remote stats message
                    # TODO: These should be in a different class
                    if (
                        isinstance(msg, DAQJobMessageStatsRemote)
                        and msg.supervisor_id == self.config.supervisor_id
                    ):
                        self.daq_job_remote_stats = msg.stats
                    # Update stats
                    self.get_daq_job_stats(
                        self.daq_job_stats, type(thread.daq_job)
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
            for daq_job_thread in self.daq_job_processes:
                daq_job = daq_job_thread.daq_job
                # Send if message is allowed for this DAQ Job
                if any(
                    isinstance(message, msg_type)
                    for msg_type in daq_job.allowed_message_in_types
                ):
                    # Drop message type that is not supported by DAQJobStore
                    if isinstance(daq_job, DAQJobStore) and not daq_job.can_store(
                        message
                    ):
                        continue
                    daq_job.message_in.put_nowait(
                        message  # , timeout=DAQ_JOB_QUEUE_ACTION_TIMEOUT
                    )

                    # Update stats
                    stats = self.get_daq_job_stats(self.daq_job_stats, type(daq_job))
                    stats.message_in_stats.increase()

                    # Do not do if Mac OS X
                    if sys.platform != "darwin":
                        stats.message_in_queue_stats.set(daq_job.message_in.qsize())
                        stats.message_out_queue_stats.set(daq_job.message_out.qsize())

    def warn_for_lack_of_daq_jobs(self):
        DAQ_JOB_ABSENT_WARNINGS = {
            DAQJobStore: "No store job found, data will not be stored",
            DAQJobAlert: "No alert job found, alerts will not be sent",
        }

        for daq_job_type, warning_message in DAQ_JOB_ABSENT_WARNINGS.items():
            if not any(
                x for x in self.daq_job_processes if isinstance(x.daq_job, daq_job_type)
            ):
                self._logger.warning(warning_message)

    def _load_supervisor_config(self):
        if not os.path.exists(SUPERVISOR_CONFIG_FILE_PATH):
            self._logger.warning(
                f"No supervisor config file found at '{SUPERVISOR_CONFIG_FILE_PATH}', using default config"
            )
            return SupervisorConfig(supervisor_id=platform.node())

        with open(SUPERVISOR_CONFIG_FILE_PATH, "rb") as f:
            return msgspec.toml.decode(f.read(), type=SupervisorConfig)

    def _get_supervisor_daq_job_info(self):
        assert self.config is not None
        return DAQJobInfo(
            daq_job_type="Supervisor",
            daq_job_class_name="Supervisor",
            supervisor_config=self.config,
            unique_id=self.config.supervisor_id,
            instance_id=0,
        )
