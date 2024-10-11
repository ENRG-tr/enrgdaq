from daq.base import DAQJob
from daq.caen.n1081b import DAQJobN1081B
from daq.store.csv import DAQJobStoreCSV
from daq.store.root import DAQJobStoreROOT
from daq.test_job import DAQJobTest

DAQ_JOB_TYPE_TO_CLASS: dict[str, type[DAQJob]] = {
    "n1081b": DAQJobN1081B,
    "test": DAQJobTest,
    "store_csv": DAQJobStoreCSV,
    "store_root": DAQJobStoreROOT,
}
