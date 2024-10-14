from daq.alert.alert_slack import DAQJobAlertSlack
from daq.base import DAQJob
from daq.jobs.caen.n1081b import DAQJobN1081B
from daq.jobs.handle_alerts import DAQJobHandleAlerts
from daq.jobs.handle_stats import DAQJobHandleStats
from daq.jobs.healthcheck import DAQJobHealthcheck
from daq.jobs.serve_http import DAQJobServeHTTP
from daq.jobs.store.csv import DAQJobStoreCSV
from daq.jobs.store.root import DAQJobStoreROOT
from daq.jobs.test_job import DAQJobTest

DAQ_JOB_TYPE_TO_CLASS: dict[str, type[DAQJob]] = {
    "n1081b": DAQJobN1081B,
    "test": DAQJobTest,
    "store_csv": DAQJobStoreCSV,
    "store_root": DAQJobStoreROOT,
    "serve_http": DAQJobServeHTTP,
    "handle_stats": DAQJobHandleStats,
    "handle_alerts": DAQJobHandleAlerts,
    "alert_slack": DAQJobAlertSlack,
    "healthcheck": DAQJobHealthcheck,
}
