import glob
import threading

import tomllib

from daq.caen.n1081b import DAQJobN1081B
from daq.models import DAQJob, DAQJobConfig

DAQ_JOB_TYPE_TO_CLASS: dict[str, type[DAQJob]] = {
    "n1081b": DAQJobN1081B,
}


def build_daq_job(toml_config: dict) -> DAQJob:
    generic_daq_job_config = DAQJobConfig.from_dict(toml_config)

    if generic_daq_job_config.daq_job_type not in DAQ_JOB_TYPE_TO_CLASS:
        raise Exception(f"Invalid DAQ job type: {generic_daq_job_config.daq_job_type}")

    # Get DAQ and DAQ config clasess based on daq_job_type
    daq_job_class = DAQ_JOB_TYPE_TO_CLASS[generic_daq_job_config.daq_job_type]
    daq_job_config_class = daq_job_class.config_type

    # Load the config in
    config = daq_job_config_class.schema().load(toml_config)

    return daq_job_class(config)


def load_daq_jobs(job_config_dir: str) -> list[DAQJob]:
    jobs = []
    job_files = glob.glob(f"{job_config_dir}/*.toml")
    for job_file in job_files:
        with open(job_file, "rb") as f:
            job_config = tomllib.load(f)

        jobs.append(build_daq_job(job_config))

    return jobs


def start_daq_job(daq_job: DAQJob) -> threading.Thread:
    thread = threading.Thread(target=daq_job.start, daemon=True)
    thread.start()

    return thread


def start_daq_jobs(daq_jobs: list[DAQJob]) -> list[threading.Thread]:
    threads = []
    for daq_job in daq_jobs:
        threads.append(start_daq_job(daq_job))

    return threads
