import logging
import time

import coloredlogs

from daq.daq_job import DAQJob, parse_store_config, start_daq_job
from daq.store.csv import DAQJobStoreCSV
from daq.store.models import DAQJobMessageStore

coloredlogs.install(
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S",
)

daq_job_thread = start_daq_job(DAQJobStoreCSV({}))
_test_daq_job = DAQJob({})
while True:
    daq_job_thread.daq_job.message_in.put_nowait(
        DAQJobMessageStore(
            daq_job=_test_daq_job,
            keys=["a", "b", "c"],
            data=[["1", "2", "3"], ["4", "5", "6"]],
            store_config=parse_store_config(
                {
                    "daq_job_store_type": "csv",
                    "file_path": "test.csv",
                    "add_date": True,
                }
            ),
        )
    )
    time.sleep(1)
