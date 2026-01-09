import glob
import logging
import os
import platform
from multiprocessing import Process, get_context
from typing import Any

import msgspec

from enrgdaq.daq.base import DAQJob, DAQJobProcess
from enrgdaq.daq.models import DAQJobConfig
from enrgdaq.daq.types import get_daq_job_class
from enrgdaq.models import SupervisorInfo

SUPERVISOR_CONFIG_FILE_NAME = "supervisor.toml"

daq_job_instance_id = 0

DAQ_JOB_PROCESS_QUEUE_MAX_SIZE = 100


def _create_daq_job_process(
    daq_job_cls: type[DAQJob],
    config: DAQJobConfig,
    supervisor_info: SupervisorInfo,
    raw_config: str = "",
    log_queue: Any = None,
) -> DAQJobProcess:
    global daq_job_instance_id
    process = DAQJobProcess(
        daq_job_cls=daq_job_cls,
        supervisor_info=supervisor_info,
        config=config,
        process=None,
        instance_id=daq_job_instance_id,
        raw_config=raw_config,
        log_queue=log_queue,
    )
    daq_job_instance_id += 1
    return process


def build_daq_job(toml_config: bytes, supervisor_info: SupervisorInfo) -> DAQJobProcess:
    generic_daq_job_config = msgspec.toml.decode(toml_config, type=DAQJobConfig)
    daq_job_class = get_daq_job_class(
        generic_daq_job_config.daq_job_type, warn_deprecated=True
    )

    if daq_job_class is None:
        raise Exception(f"Invalid DAQ job type: {generic_daq_job_config.daq_job_type}")

    # Get DAQ config clase based on daq_job_type
    daq_job_config_class: type[DAQJobConfig] = daq_job_class.config_type

    # Load the config in
    config = msgspec.toml.decode(toml_config, type=daq_job_config_class)

    return _create_daq_job_process(
        daq_job_class, config, supervisor_info, toml_config.decode()
    )


def rebuild_daq_job(
    daq_job_process: DAQJobProcess, supervisor_info: SupervisorInfo
) -> DAQJobProcess:
    return _create_daq_job_process(
        daq_job_process.daq_job_cls,
        daq_job_process.config,
        supervisor_info,
        log_queue=daq_job_process.log_queue,
    )


def load_daq_jobs(
    job_config_dir: str, supervisor_info: SupervisorInfo
) -> list[DAQJobProcess]:
    jobs = []
    job_files = glob.glob(os.path.join(job_config_dir, "*.toml"))
    for job_file in job_files:
        # Skip the supervisor config file
        if os.path.basename(job_file) == SUPERVISOR_CONFIG_FILE_NAME:
            continue

        with open(job_file, "rb") as f:
            job_config_raw = f.read()

        jobs.append(build_daq_job(job_config_raw, supervisor_info))

    return jobs


def start_daq_job(daq_job_process: DAQJobProcess) -> DAQJobProcess:
    logging.info(f"Starting {daq_job_process.daq_job_cls.__name__}")

    job_multiprocessing_method = getattr(
        daq_job_process.daq_job_cls, "multiprocessing_method", "default"
    )
    # Use 'fork' method on Unix systems (including macOS) by default to avoid semaphore lock issues
    # when pickling/unpickling Queue objects during process spawn, but allow individual jobs to override
    if platform.system() in ["Darwin", "Linux"]:
        if job_multiprocessing_method == "spawn":
            # Use default Process (which will use spawn on macOS)
            process = Process(target=daq_job_process.start, daemon=True)
        elif job_multiprocessing_method == "fork":
            # Explicitly use fork context
            ctx = get_context("fork")
            process = ctx.Process(target=daq_job_process.start, daemon=True)
        else:  # default behavior
            # Use fork for better compatibility with most DAQ jobs
            ctx = get_context("fork")
            process = ctx.Process(target=daq_job_process.start, daemon=True)
    else:
        # Use default Process on Windows (which doesn't support fork)
        process = Process(target=daq_job_process.start, daemon=True)

    process.start()
    daq_job_process.process = process  # type: ignore
    try:
        """daq_job_info_message = daq_job_process.message_out.get(timeout=5000)
        if isinstance(daq_job_info_message, DAQJobMessageJobStarted):
            daq_job_process.daq_job_info = daq_job_info_message.daq_job_info
        else:
            raise Exception("Initial message of DAQJob was not DAQJobMessageJobStarted")"""
    except Exception as e:
        logging.error(
            f"Could not get DAQ job info for {daq_job_process.daq_job_cls.__name__}: {e}",
            exc_info=True,
        )
    return daq_job_process


def start_daq_jobs(daq_job_processes: list[DAQJobProcess]) -> list[DAQJobProcess]:
    processes = []
    for daq_job in daq_job_processes:
        processes.append(start_daq_job(daq_job))

    return processes
