import glob
import logging
import os
import threading

import msgspec

from daq.base import DAQJob, DAQJobThread
from daq.models import DAQJobConfig
from daq.store.models import DAQJobStoreConfig
from daq.types import DAQ_JOB_TYPE_TO_CLASS


def build_daq_job(toml_config: bytes) -> DAQJob:
    generic_daq_job_config = msgspec.toml.decode(toml_config, type=DAQJobConfig)

    if generic_daq_job_config.daq_job_type not in DAQ_JOB_TYPE_TO_CLASS:
        raise Exception(f"Invalid DAQ job type: {generic_daq_job_config.daq_job_type}")

    # Get DAQ and DAQ config clasess based on daq_job_type
    daq_job_class = DAQ_JOB_TYPE_TO_CLASS[generic_daq_job_config.daq_job_type]
    daq_job_config_class: DAQJobConfig = daq_job_class.config_type

    # Load the config in
    config = msgspec.toml.decode(toml_config, type=daq_job_config_class)

    return daq_job_class(config)


def load_daq_jobs(job_config_dir: str) -> list[DAQJob]:
    jobs = []
    job_files = glob.glob(os.path.join(job_config_dir, "*.toml"))
    for job_file in job_files:
        with open(job_file, "rb") as f:
            job_config_raw = f.read()

        jobs.append(build_daq_job(job_config_raw))

    return jobs


def start_daq_job(daq_job: DAQJob) -> DAQJobThread:
    logging.info(f"Starting {type(daq_job).__name__}")
    thread = threading.Thread(target=daq_job.start, daemon=True)
    thread.start()

    return DAQJobThread(daq_job, thread)


def restart_daq_job(daq_job: DAQJob) -> DAQJobThread:
    logging.info(f"Restarting {type(daq_job).__name__}")
    new_daq_job = type(daq_job)(daq_job.config)
    thread = threading.Thread(target=new_daq_job.start, daemon=True)
    thread.start()
    return DAQJobThread(new_daq_job, thread)


def start_daq_jobs(daq_jobs: list[DAQJob]) -> list[DAQJobThread]:
    threads = []
    for daq_job in daq_jobs:
        threads.append(start_daq_job(daq_job))

    return threads


def parse_store_config(config: dict) -> DAQJobStoreConfig:
    from daq.store.types import DAQ_STORE_CONFIG_TYPE_TO_CLASS

    if "daq_job_store_type" not in config:
        raise Exception("No daq_job_store_type specified in config")

    daq_job_store_type = config["daq_job_store_type"]
    store_config_class = DAQ_STORE_CONFIG_TYPE_TO_CLASS[daq_job_store_type]

    return store_config_class(**config)
