from typing import Any

from msgspec import Struct

from daq.base import DAQJobInfo
from daq.models import DAQJobConfig, DAQJobMessage


class DAQJobStoreConfig(Struct):
    """
    Used to store the configuration of the DAQ Job Store, usually inside DAQJobConfig.
    """

    daq_job_store_type: str


class DAQJobMessageStore(DAQJobMessage):
    store_config: dict | DAQJobStoreConfig
    daq_job_info: DAQJobInfo
    keys: list[str]
    data: list[list[Any]]
    prefix: str | None = None


class StorableDAQJobConfig(DAQJobConfig):
    store_config: dict
