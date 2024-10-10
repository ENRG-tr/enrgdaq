import time
from dataclasses import dataclass

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
    store_config: DAQJobStoreConfig
    daq_job: DAQJob


@dataclass
class StorableDAQJobConfig(DAQJobConfig):
    store_config: type[DAQJobStoreConfig]
