import logging
import time
from datetime import datetime
from queue import Empty

import coloredlogs

from daq.base import DAQJob, DAQJobThread
from daq.daq_job import load_daq_jobs, parse_store_config, start_daq_job, start_daq_jobs
from daq.models import DAQJobMessage, DAQJobStats
from daq.store.base import DAQJobStore
from daq.store.models import DAQJobMessageStore
from daq.types import DAQJobStatsDict

DAQ_SUPERVISOR_SLEEP_TIME = 0.5
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
        daq_job_threads.append(start_daq_job(thread.daq_job))

    # Get messages from DAQ Jobs
    daq_messages_out = get_messages_from_daq_jobs(daq_job_threads, daq_job_stats)

    # Send messages to appropriate DAQ Jobs
    send_messages_to_daq_jobs(daq_job_threads, daq_messages_out, daq_job_stats)

    return daq_job_threads, daq_job_stats


def get_or_create_daq_job_stats(
    daq_job_stats: DAQJobStatsDict, daq_job_type: type[DAQJob]
) -> DAQJobStats:
    if daq_job_type not in daq_job_stats:
        daq_job_stats[daq_job_type] = DAQJobStats(
            message_in_count=0,
            message_out_count=0,
            last_message_in_date=None,
            last_message_out_date=None,
        )
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
                stats = get_or_create_daq_job_stats(daq_job_stats, type(thread.daq_job))
                stats.message_out_count += 1
                stats.last_message_out_date = datetime.now()
        except Empty:
            pass
    return res


def send_messages_to_daq_jobs(
    daq_job_threads: list[DAQJobThread],
    daq_messages: list[DAQJobMessage],
    daq_job_stats: DAQJobStatsDict,
):
    for message in daq_messages:
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
                stats = get_or_create_daq_job_stats(daq_job_stats, type(daq_job))
                stats.message_in_count += 1
                stats.last_message_in_date = datetime.now()


if __name__ == "__main__":
    coloredlogs.install(
        level=logging.DEBUG,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    daq_job_threads = start_daq_job_threads()
    daq_job_stats: DAQJobStatsDict = {}

    if not any(x for x in daq_job_threads if isinstance(x.daq_job, DAQJobStore)):
        logging.warning("No store job found, data will not be stored")

    while True:
        try:
            daq_job_threads, daq_job_stats = loop(daq_job_threads, daq_job_stats)
            time.sleep(DAQ_SUPERVISOR_SLEEP_TIME)
        except KeyboardInterrupt:
            logging.warning("KeyboardInterrupt received, cleaning up")
            for daq_job_thread in daq_job_threads:
                daq_job_thread.daq_job.__del__()
            break
