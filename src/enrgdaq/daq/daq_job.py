import glob
import logging
import os
from multiprocessing import Process, Queue
from pathlib import Path

import msgspec

from enrgdaq.daq.base import DAQJobProcess
from enrgdaq.daq.models import DAQJobConfig
from enrgdaq.daq.types import get_daq_job_class
from enrgdaq.models import SupervisorConfig

SUPERVISOR_CONFIG_FILE_PATH = "configs/supervisor.toml"

daq_job_instance_id = 0


def build_daq_job(
    toml_config: bytes, supervisor_config: SupervisorConfig
) -> DAQJobProcess:
    global daq_job_instance_id
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

    process = DAQJobProcess(
        daq_job_cls=daq_job_class,
        supervisor_config=supervisor_config.clone(),
        config=config,
        message_in=Queue(),
        message_out=Queue(),
        process=None,
        instance_id=daq_job_instance_id,
    )
    daq_job_instance_id += 1
    return process


def load_daq_jobs(
    job_config_dir: str, supervisor_config: SupervisorConfig
) -> list[DAQJobProcess]:
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


def start_daq_job(daq_job_process: DAQJobProcess) -> DAQJobProcess:
    logging.info(f"Starting {daq_job_process.daq_job_cls.__name__}")
    process = Process(target=daq_job_process.start, daemon=True)
    process.start()
    daq_job_process.process = process
    return daq_job_process


def start_daq_jobs(daq_job_processes: list[DAQJobProcess]) -> list[DAQJobProcess]:
    processes = []
    for daq_job in daq_job_processes:
        processes.append(start_daq_job(daq_job))

    return processes
