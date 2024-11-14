import logging
from typing import Optional

from daq.alert.alert_slack import DAQJobAlertSlack
from daq.base import DAQJob
from daq.jobs.caen.n1081b import DAQJobN1081B
from daq.jobs.handle_alerts import DAQJobHandleAlerts
from daq.jobs.handle_stats import DAQJobHandleStats
from daq.jobs.healthcheck import DAQJobHealthcheck
from daq.jobs.remote import DAQJobRemote
from daq.jobs.serve_http import DAQJobServeHTTP
from daq.jobs.store.csv import DAQJobStoreCSV
from daq.jobs.store.root import DAQJobStoreROOT
from daq.jobs.test_job import DAQJobTest
from utils.subclasses import all_subclasses

ALL_DAQ_JOBS = all_subclasses(DAQJob)


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
    "remote": DAQJobRemote,
}


def get_daq_job_class(
    daq_job_type: str, warn_deprecated: bool = False
) -> Optional[type[DAQJob]]:
    daq_job_class = None
    if daq_job_type in DAQ_JOB_TYPE_TO_CLASS:
        daq_job_class = DAQ_JOB_TYPE_TO_CLASS[daq_job_type]
        if warn_deprecated:
            logging.warning(
                f"DAQ job type '{daq_job_type}' is deprecated, please use '{daq_job_class.__name__}' instead"
            )
    else:
        for daq_job in ALL_DAQ_JOBS:
            if daq_job.__name__ == daq_job_type:
                daq_job_class = daq_job
    return daq_job_class
