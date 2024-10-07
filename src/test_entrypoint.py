import time

from daq.daq_job import load_daq_jobs, start_daq_jobs

daq_jobs = load_daq_jobs("configs/")
threads = start_daq_jobs(daq_jobs)

while True:
    any_thread_alive = any(t.is_alive() for t in threads)
    if not any_thread_alive:
        break

    time.sleep(1)
