import logging
import time

from daq.daq_job import load_daq_jobs, start_daq_jobs
from daq.store.models import DAQJobStore

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] [%(name)s.%(funcName)s:%(lineno)d] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


daq_jobs = load_daq_jobs("configs/")
daq_job_threads = start_daq_jobs(daq_jobs)

store_jobs = [x for x in daq_jobs if isinstance(x, DAQJobStore)]

if len(store_jobs) == 0:
    logging.warning("No store job found, data will not be stored")

while True:
    any_thread_alive = any(t.thread.is_alive() for t in daq_job_threads)
    if not any_thread_alive:
        break

    time.sleep(1)
