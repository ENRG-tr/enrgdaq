from dataclasses import dataclass
from typing import Any, Optional

from dataclasses_json import DataClassJsonMixin

from daq.base import DAQJob
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
    daq_job: Optional[DAQJob]
    keys: list[str]
    data: list[list[Any]]
    prefix: str | None = None

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["daq_job"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.daq_job = None  # type: ignore


@dataclass
class StorableDAQJobConfig(DAQJobConfig):
    store_config: dict
