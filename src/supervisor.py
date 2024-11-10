import logging
import os
import platform
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from queue import Empty

import msgspec

from daq.alert.base import DAQJobAlert
from daq.base import DAQJob, DAQJobThread
from daq.daq_job import (
    SUPERVISOR_CONFIG_FILE_PATH,
    load_daq_jobs,
    restart_daq_job,
    start_daq_jobs,
)
from daq.jobs.handle_stats import DAQJobMessageStats, DAQJobStatsDict
from daq.models import DAQJobConfig, DAQJobMessage, DAQJobStats
from daq.store.base import DAQJobStore
from models import SupervisorConfig

DAQ_SUPERVISOR_SLEEP_TIME_SECONDS = 0.2
DAQ_JOB_QUEUE_ACTION_TIMEOUT = 0.1


@dataclass
class RestartDAQJobSchedule:
    daq_job_type: type[DAQJob]
    daq_job_config: DAQJobConfig
    restart_at: datetime


class Supervisor:
    daq_job_threads: list[DAQJobThread]
    daq_job_stats: DAQJobStatsDict
    restart_schedules: list[RestartDAQJobSchedule]

    def init(self):
        self.config = self._load_supervisor_config()
        # Change logging name based on supervisor id
        logging.getLogger().name = f"Supervisor({self.config.supervisor_id})"

        self.restart_schedules = []
        self.daq_job_threads = self.start_daq_job_threads()
        self.daq_job_stats: DAQJobStatsDict = {
            type(thread.daq_job): DAQJobStats() for thread in self.daq_job_threads
        }
        self.warn_for_lack_of_daq_jobs()

    def start_daq_job_threads(self) -> list[DAQJobThread]:
        return start_daq_jobs(load_daq_jobs("configs/", self.config))

    def run(self):
        while True:
            try:
                self.loop()
                time.sleep(DAQ_SUPERVISOR_SLEEP_TIME_SECONDS)
            except KeyboardInterrupt:
                logging.warning("KeyboardInterrupt received, cleaning up")
                for daq_job_thread in self.daq_job_threads:
                    daq_job_thread.daq_job.__del__()
                break

    def loop(self):
        # Remove dead threads
        dead_threads = [t for t in self.daq_job_threads if not t.thread.is_alive()]
        # Clean up dead threads
        self.daq_job_threads = [
            t for t in self.daq_job_threads if t not in dead_threads
        ]

        # Get restart schedules for dead jobs
        self.restart_schedules.extend(self.get_restart_schedules(dead_threads))

        # Restart jobs that have stopped or are scheduled to restart
        self.restart_daq_jobs()

        # Get messages from DAQ Jobs
        daq_messages_out = self.get_messages_from_daq_jobs()

        # Add supervisor messages
        daq_messages_out.extend(self.get_supervisor_messages())

        # Send messages to appropriate DAQ Jobs
        self.send_messages_to_daq_jobs(daq_messages_out)

    def get_restart_schedules(self, dead_threads: list[DAQJobThread]):
        res = []
        for thread in dead_threads:
            restart_offset = getattr(thread.daq_job, "restart_offset", None)
            if not isinstance(restart_offset, timedelta):
                restart_offset = timedelta(seconds=0)
            else:
                logging.info(
                    f"Scheduling restart of {type(thread.daq_job).__name__} in {restart_offset.total_seconds()} seconds"
                )
            res.append(
                RestartDAQJobSchedule(
                    daq_job_type=type(thread.daq_job),
                    daq_job_config=thread.daq_job.config,
                    restart_at=datetime.now() + restart_offset,
                )
            )
            thread.daq_job.free()
        return res

    def restart_daq_jobs(self):
        schedules_to_remove = []
        for restart_schedule in self.restart_schedules:
            if datetime.now() < restart_schedule.restart_at:
                continue
            self.daq_job_threads.append(
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

    def get_supervisor_messages(self):
        messages = []

        # Send stats message
        messages.append(DAQJobMessageStats(stats=self.daq_job_stats))
        return messages

    def get_daq_job_stats(
        self, daq_job_stats: DAQJobStatsDict, daq_job_type: type[DAQJob]
    ) -> DAQJobStats:
        if daq_job_type not in daq_job_stats:
            daq_job_stats[daq_job_type] = DAQJobStats()
        return daq_job_stats[daq_job_type]

    def get_messages_from_daq_jobs(self) -> list[DAQJobMessage]:
        res = []
        for thread in self.daq_job_threads:
            try:
                while True:
                    msg = thread.daq_job.message_out.get_nowait()
                    if msg.daq_job_info is None:
                        msg.daq_job_info = thread.daq_job.info
                    res.append(msg)
                    # Update stats
                    self.get_daq_job_stats(
                        self.daq_job_stats, type(thread.daq_job)
                    ).message_out_stats.increase()
            except Empty:
                pass
        return res

    def send_messages_to_daq_jobs(self, daq_messages: list[DAQJobMessage]):
        for message in daq_messages:
            for daq_job_thread in self.daq_job_threads:
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
                    daq_job.message_in.put(
                        message, timeout=DAQ_JOB_QUEUE_ACTION_TIMEOUT
                    )

                    # Update stats
                    self.get_daq_job_stats(
                        self.daq_job_stats, type(daq_job)
                    ).message_in_stats.increase()

    def warn_for_lack_of_daq_jobs(self):
        DAQ_JOB_ABSENT_WARNINGS = {
            DAQJobStore: "No store job found, data will not be stored",
            DAQJobAlert: "No alert job found, alerts will not be sent",
        }

        for daq_job_type, warning_message in DAQ_JOB_ABSENT_WARNINGS.items():
            if not any(
                x for x in self.daq_job_threads if isinstance(x.daq_job, daq_job_type)
            ):
                logging.warning(warning_message)

    def _load_supervisor_config(self):
        if not os.path.exists(SUPERVISOR_CONFIG_FILE_PATH):
            logging.warning(
                f"No supervisor config file found at '{SUPERVISOR_CONFIG_FILE_PATH}', using default config"
            )
            return SupervisorConfig(supervisor_id=platform.node())

        with open(SUPERVISOR_CONFIG_FILE_PATH, "rb") as f:
            return msgspec.toml.decode(f.read(), type=SupervisorConfig)
