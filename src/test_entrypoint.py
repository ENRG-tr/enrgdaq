import logging
import time

from daq.daq_job import load_daq_jobs, start_daq_job, start_daq_jobs
from daq.models import DAQJobMessageStop
from daq.store.models import DAQJobStore

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


daq_jobs = load_daq_jobs("configs/")
daq_job_threads = start_daq_jobs(daq_jobs)

store_jobs = [x for x in daq_jobs if isinstance(x, DAQJobStore)]

if len(store_jobs) == 0:
    logging.warning("No store job found, data will not be stored")

while True:
    dead_threads = [t for t in daq_job_threads if not t.thread.is_alive()]
    # Clean up dead threads
    daq_job_threads = [t for t in daq_job_threads if t not in dead_threads]

    # Restart jobs that have stopped
    for thread in dead_threads:
        daq_job_threads.append(start_daq_job(thread.daq_job))

    time.sleep(1)
