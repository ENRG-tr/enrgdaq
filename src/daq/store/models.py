from dataclasses import dataclass
from typing import Any

from dataclasses_json import DataClassJsonMixin

from daq.base import DAQJobInfo
from daq.models import DAQJobConfig, DAQJobMessage


@dataclass
class DAQJobStoreConfig(DataClassJsonMixin):
    """
    Used to store the configuration of the DAQ Job Store, usually inside DAQJobConfig.
    """

    daq_job_store_type: str


@dataclass
class DAQJobMessageStore(DAQJobMessage):
    store_config: dict | DAQJobStoreConfig
    daq_job_info: DAQJobInfo
    keys: list[str]
    data: list[list[Any]]
    prefix: str | None = None


@dataclass
class StorableDAQJobConfig(DAQJobConfig):
    store_config: dict
