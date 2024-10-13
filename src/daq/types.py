from typing import Dict

from daq.base import DAQJob
from daq.jobs.caen.n1081b import DAQJobN1081B
from daq.jobs.serve_http import DAQJobServeHTTP
from daq.jobs.store.csv import DAQJobStoreCSV
from daq.jobs.store.root import DAQJobStoreROOT
from daq.jobs.test_job import DAQJobTest
from daq.models import DAQJobStats

DAQ_JOB_TYPE_TO_CLASS: dict[str, type[DAQJob]] = {
    "n1081b": DAQJobN1081B,
    "test": DAQJobTest,
    "store_csv": DAQJobStoreCSV,
    "store_root": DAQJobStoreROOT,
    "serve_http": DAQJobServeHTTP,
}

DAQJobStatsDict = Dict[type[DAQJob], DAQJobStats]
