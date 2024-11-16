from enrgdaq.daq.jobs.store.csv import DAQJobStoreConfigCSV
from enrgdaq.daq.jobs.store.root import DAQJobStoreConfigROOT

DAQ_STORE_CONFIG_TYPE_TO_CLASS = {
    "csv": DAQJobStoreConfigCSV,
    "root": DAQJobStoreConfigROOT,
}
