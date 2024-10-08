import logging
import time

from daq.daq_job import load_daq_jobs, start_daq_jobs

# set logging level to debug
logging.basicConfig(level=logging.DEBUG)

daq_jobs = load_daq_jobs("configs/")
daq_job_threads = start_daq_jobs(daq_jobs)

while True:
    any_thread_alive = any(t.thread.is_alive() for t in daq_job_threads)
    if not any_thread_alive:
        break

    time.sleep(1)
