from typing import Any, Optional

from msgspec import Struct

from daq.base import DAQJobInfo
from daq.models import DAQJobConfig, DAQJobMessage


class DAQJobStoreConfig(Struct, dict=True):
    """
    Used to store the configuration of the DAQ Job Store, usually inside DAQJobConfig.
    """

    csv: "Optional[DAQJobStoreConfigCSV]" = None
    root: "Optional[DAQJobStoreConfigROOT]" = None

    def has_store_config(self, store_type: Any) -> bool:
        for key in dir(self):
            if key.startswith("_"):
                continue
            value = getattr(self, key)
            if isinstance(value, store_type):
                return True
        return False


class DAQJobMessageStore(DAQJobMessage):
    store_config: DAQJobStoreConfig
    daq_job_info: DAQJobInfo
    keys: list[str]
    data: list[list[Any]]
    prefix: str | None = None


class StorableDAQJobConfig(DAQJobConfig):
    store_config: DAQJobStoreConfig


class DAQJobStoreTarget(Struct):
    instances: Optional[list["DAQJobStoreTargetInstance"]] = None
    jobs: Optional[list["DAQJobStoreTargetJob"]] = None


class DAQJobStoreTargetInstance(Struct):
    supervisor_id: Optional[str] = None
    is_self: Optional[bool] = None


class DAQJobStoreTargetJob(Struct):
    job_name: str


class DAQJobStoreConfigBase(Struct, kw_only=True):
    target: Optional[DAQJobStoreTarget] = None


class DAQJobStoreConfigCSV(DAQJobStoreConfigBase):
    file_path: str
    add_date: bool
    overwrite: Optional[bool] = None


class DAQJobStoreConfigROOT(DAQJobStoreConfigBase):
    file_path: str
    add_date: bool
