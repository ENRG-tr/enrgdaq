import glob
import logging
import os
import threading
from pathlib import Path

import msgspec

from daq.base import DAQJob, DAQJobThread
from daq.models import DAQJobConfig
from daq.types import get_daq_job_class
from models import SupervisorConfig

SUPERVISOR_CONFIG_FILE_PATH = "configs/supervisor.toml"


def build_daq_job(toml_config: bytes, supervisor_config: SupervisorConfig) -> DAQJob:
    generic_daq_job_config = msgspec.toml.decode(toml_config, type=DAQJobConfig)
    daq_job_class = get_daq_job_class(
        generic_daq_job_config.daq_job_type, warn_deprecated=True
    )

    if daq_job_class is None:
        raise Exception(f"Invalid DAQ job type: {generic_daq_job_config.daq_job_type}")

    # Get DAQ config clase based on daq_job_type
    daq_job_config_class: DAQJobConfig = daq_job_class.config_type

    # Load the config in
    config = msgspec.toml.decode(toml_config, type=daq_job_config_class)

    return daq_job_class(config, supervisor_config=supervisor_config.clone())


def load_daq_jobs(
    job_config_dir: str, supervisor_config: SupervisorConfig
) -> list[DAQJob]:
    jobs = []
    job_files = glob.glob(os.path.join(job_config_dir, "*.toml"))
    for job_file in job_files:
        # Skip the supervisor config file
        if Path(job_file) == Path(SUPERVISOR_CONFIG_FILE_PATH):
            continue

        with open(job_file, "rb") as f:
            job_config_raw = f.read()

        jobs.append(build_daq_job(job_config_raw, supervisor_config))

    return jobs


def start_daq_job(daq_job: DAQJob) -> DAQJobThread:
    logging.info(f"Starting {type(daq_job).__name__}")
    thread = threading.Thread(target=daq_job.start, daemon=True)
    thread.start()

    return DAQJobThread(daq_job, thread)


def restart_daq_job(
    daq_job_type: type[DAQJob],
    daq_job_config: DAQJobConfig,
    supervisor_config: SupervisorConfig,
) -> DAQJobThread:
    logging.info(f"Restarting {daq_job_type.__name__}")
    new_daq_job = daq_job_type(
        daq_job_config, supervisor_config=supervisor_config.clone()
    )
    thread = threading.Thread(target=new_daq_job.start, daemon=True)
    thread.start()
    return DAQJobThread(new_daq_job, thread)


def start_daq_jobs(daq_jobs: list[DAQJob]) -> list[DAQJobThread]:
    threads = []
    for daq_job in daq_jobs:
        threads.append(start_daq_job(daq_job))

    return threads
