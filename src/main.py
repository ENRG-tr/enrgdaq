import logging
import time
from queue import Empty

import coloredlogs

from daq.alert.base import DAQJobAlert
from daq.base import DAQJob, DAQJobThread
from daq.daq_job import (
    load_daq_jobs,
    parse_store_config,
    restart_daq_job,
    start_daq_jobs,
)
from daq.jobs.handle_stats import DAQJobMessageStats, DAQJobStatsDict
from daq.models import DAQJobMessage, DAQJobStats
from daq.store.base import DAQJobStore
from daq.store.models import DAQJobMessageStore

DAQ_SUPERVISOR_SLEEP_TIME = 0.2
DAQ_JOB_QUEUE_ACTION_TIMEOUT = 0.1


def start_daq_job_threads() -> list[DAQJobThread]:
    return start_daq_jobs(load_daq_jobs("configs/"))


def loop(
    daq_job_threads: list[DAQJobThread],
    daq_job_stats: DAQJobStatsDict,
) -> tuple[list[DAQJobThread], DAQJobStatsDict]:
    # Remove dead threads
    dead_threads = [t for t in daq_job_threads if not t.thread.is_alive()]
    # Clean up dead threads
    daq_job_threads = [t for t in daq_job_threads if t not in dead_threads]

    # Restart jobs that have stopped
    for thread in dead_threads:
        daq_job_threads.append(restart_daq_job(thread.daq_job))
        # Update restart stats
        get_daq_job_stats(daq_job_stats, type(thread.daq_job)).restart_stats.increase()

    # Get messages from DAQ Jobs
    daq_messages_out = get_messages_from_daq_jobs(daq_job_threads, daq_job_stats)

    # Add supervisor messages
    daq_messages_out.extend(get_supervisor_messages(daq_job_threads, daq_job_stats))

    # Send messages to appropriate DAQ Jobs
    send_messages_to_daq_jobs(daq_job_threads, daq_messages_out, daq_job_stats)

    return daq_job_threads, daq_job_stats


def get_supervisor_messages(
    daq_job_threads: list[DAQJobThread], daq_job_stats: DAQJobStatsDict
):
    messages = []

    # Send stats message
    messages.append(DAQJobMessageStats(stats=daq_job_stats))
    return messages


def get_daq_job_stats(
    daq_job_stats: DAQJobStatsDict, daq_job_type: type[DAQJob]
) -> DAQJobStats:
    if daq_job_type not in daq_job_stats:
        daq_job_stats[daq_job_type] = DAQJobStats()
    return daq_job_stats[daq_job_type]


def get_messages_from_daq_jobs(
    daq_job_threads: list[DAQJobThread], daq_job_stats: DAQJobStatsDict
) -> list[DAQJobMessage]:
    res = []
    for thread in daq_job_threads:
        try:
            while True:
                res.append(
                    thread.daq_job.message_out.get(timeout=DAQ_JOB_QUEUE_ACTION_TIMEOUT)
                )
                # Update stats
                get_daq_job_stats(
                    daq_job_stats, type(thread.daq_job)
                ).message_out_stats.increase()
        except Empty:
            pass
    return res


def send_messages_to_daq_jobs(
    daq_job_threads: list[DAQJobThread],
    daq_messages: list[DAQJobMessage],
    daq_job_stats: DAQJobStatsDict,
):
    for message in daq_messages:
        # TODO: Make this into a generalized interface
        if isinstance(message, DAQJobMessageStore) and isinstance(
            message.store_config, dict
        ):
            # Parse store config of DAQJobMessageStore
            message.store_config = parse_store_config(message.store_config)

        for daq_job_thread in daq_job_threads:
            daq_job = daq_job_thread.daq_job
            # Send if message is allowed for this DAQ Job
            if any(
                isinstance(message, msg_type)
                for msg_type in daq_job.allowed_message_in_types
            ):
                # Drop message type that is not supported by DAQJobStore
                if isinstance(daq_job, DAQJobStore) and not daq_job.can_store(message):
                    continue
                daq_job.message_in.put(message, timeout=DAQ_JOB_QUEUE_ACTION_TIMEOUT)

                # Update stats
                get_daq_job_stats(
                    daq_job_stats, type(daq_job)
                ).message_in_stats.increase()


def warn_for_lack_of_daq_jobs(daq_job_threads: list[DAQJobThread]):
    DAQ_JOB_ABSENT_WARNINGS = {
        DAQJobStore: "No store job found, data will not be stored",
        DAQJobAlert: "No alert job found, alerts will not be sent",
    }

    for daq_job_type, warning_message in DAQ_JOB_ABSENT_WARNINGS.items():
        if not any(x for x in daq_job_threads if isinstance(x.daq_job, daq_job_type)):
            logging.warning(warning_message)


if __name__ == "__main__":
    coloredlogs.install(
        level=logging.DEBUG,
        datefmt="%Y-%m-%d %H:%M:%S",
        fmt="%(asctime)s %(hostname)s %(name)s %(levelname)s %(message)s",
    )
    daq_job_threads = start_daq_job_threads()
    daq_job_stats: DAQJobStatsDict = {
        type(thread.daq_job): DAQJobStats() for thread in daq_job_threads
    }

    warn_for_lack_of_daq_jobs(daq_job_threads)

    while True:
        try:
            daq_job_threads, daq_job_stats = loop(daq_job_threads, daq_job_stats)
            time.sleep(DAQ_SUPERVISOR_SLEEP_TIME)
        except KeyboardInterrupt:
            logging.warning("KeyboardInterrupt received, cleaning up")
            for daq_job_thread in daq_job_threads:
                daq_job_thread.daq_job.__del__()
            break
