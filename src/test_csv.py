import logging
import time

import coloredlogs

from daq.daq_job import DAQJob, start_daq_job
from daq.store.csv import DAQJobMessageStoreCSV, DAQJobStoreConfigCSV, DAQJobStoreCSV

coloredlogs.install(
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S",
)

daq_job_thread = start_daq_job(DAQJobStoreCSV({}))
_test_daq_job = DAQJob({})
while True:
    daq_job_thread.daq_job.message_in.put_nowait(
        DAQJobMessageStoreCSV(
            daq_job=_test_daq_job,
            header=["a", "b", "c"],
            data=[["1", "2", "3"], ["4", "5", "6"]],
            store_config=DAQJobStoreConfigCSV(
                daq_job_store_type="",
                file_path="test.csv",
                add_date=True,
            ),
        )
    )
    time.sleep(1)
