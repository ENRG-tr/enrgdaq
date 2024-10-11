import logging
import time
from queue import Empty

import coloredlogs

from daq.daq_job import load_daq_jobs, parse_store_config, start_daq_job, start_daq_jobs
from daq.store.base import DAQJobStore
from daq.store.models import DAQJobMessageStore

coloredlogs.install(
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S",
)

DAQ_SUPERVISOR_SLEEP_TIME = 0.5
DAQ_JOB_QUEUE_ACTION_TIMEOUT = 0.1

daq_jobs = load_daq_jobs("configs/")
daq_job_threads = start_daq_jobs(daq_jobs)

if not any(x for x in daq_jobs if isinstance(x, DAQJobStore)):
    logging.warning("No store job found, data will not be stored")


def loop():
    global daq_job_threads
    dead_threads = [t for t in daq_job_threads if not t.thread.is_alive()]
    # Clean up dead threads
    daq_job_threads = [t for t in daq_job_threads if t not in dead_threads]

    # Restart jobs that have stopped
    for thread in dead_threads:
        daq_job_threads.append(start_daq_job(thread.daq_job))

    # Get messages from DAQ Jobs
    daq_messages = []
    for thread in daq_job_threads:
        try:
            daq_messages.append(
                thread.daq_job.message_out.get(timeout=DAQ_JOB_QUEUE_ACTION_TIMEOUT)
            )
        except Empty:
            pass

    # Send messages to appropriate DAQ Jobs
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
                # Drop message type that is not supported by this DAQ Job
                if isinstance(daq_job, DAQJobStore) and not daq_job.can_store(message):
                    continue

                daq_job.message_in.put(message, timeout=DAQ_JOB_QUEUE_ACTION_TIMEOUT)

    time.sleep(DAQ_SUPERVISOR_SLEEP_TIME)


while True:
    try:
        loop()
    except KeyboardInterrupt:
        logging.warning("KeyboardInterrupt received, cleaning up")
        for daq_job_thread in daq_job_threads:
            daq_job_thread.daq_job.__del__()
        break
