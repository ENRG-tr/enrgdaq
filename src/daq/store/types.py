from daq.store.csv import DAQJobStoreConfigCSV
from daq.store.root import DAQJobStoreConfigROOT

DAQ_STORE_CONFIG_TYPE_TO_CLASS = {
    "csv": DAQJobStoreConfigCSV,
    "root": DAQJobStoreConfigROOT,
}
