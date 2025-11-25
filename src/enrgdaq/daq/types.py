import logging
from typing import Optional

from enrgdaq.daq.alert.alert_slack import DAQJobAlertSlack
from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.jobs.handle_alerts import DAQJobHandleAlerts
from enrgdaq.daq.jobs.handle_stats import DAQJobHandleStats
from enrgdaq.daq.jobs.healthcheck import DAQJobHealthcheck
from enrgdaq.daq.jobs.remote import DAQJobRemote
from enrgdaq.daq.jobs.serve_http import DAQJobServeHTTP
from enrgdaq.daq.jobs.store.csv import DAQJobStoreCSV
from enrgdaq.daq.jobs.store.root import DAQJobStoreROOT
from enrgdaq.daq.jobs.test_job import DAQJobTest
from enrgdaq.utils.subclasses import all_subclasses


def get_all_daq_job_types():
    return all_subclasses(DAQJob)


DAQ_JOB_TYPE_TO_CLASS: dict[str, type[DAQJob]] = {
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
    """
    Gets the DAQJob class for a given DAQ job type.

    Args:
        daq_job_type (str): The type of the DAQ job.
        warn_deprecated (bool): Whether to warn if the DAQ job type is deprecated.

    Returns:
        Optional[type[DAQJob]]: The DAQJob class for the given DAQ job type, or None if not found.
    """

    daq_job_class = None
    if daq_job_type in DAQ_JOB_TYPE_TO_CLASS:
        daq_job_class = DAQ_JOB_TYPE_TO_CLASS[daq_job_type]
        if warn_deprecated:
            logging.warning(
                f"DAQ job type '{daq_job_type}' is deprecated, please use '{daq_job_class.__name__}' instead"
            )
    else:
        for daq_job in get_all_daq_job_types():
            if daq_job.__name__ == daq_job_type:
                daq_job_class = daq_job
    return daq_job_class
