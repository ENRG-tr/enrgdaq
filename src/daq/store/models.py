from typing import Any, Optional

from msgspec import Struct

from daq.models import REMOTE_TOPIC_VOID, DAQJobConfig, DAQJobMessage


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
    keys: list[str]
    data: list[list[Any]]
    prefix: str | None = None

    def __post_init__(self):
        for key in dir(self.store_config):
            value = getattr(self.store_config, key)
            if not isinstance(value, DAQJobStoreConfigBase):
                continue
            if value.remote_topic is not None:
                self.remote_topic = value.remote_topic
            if getattr(value, "remote_disable", False):
                self.remote_topic = REMOTE_TOPIC_VOID


class StorableDAQJobConfig(DAQJobConfig):
    store_config: DAQJobStoreConfig


class DAQJobStoreTarget(Struct):
    instances: Optional[list["DAQJobStoreTargetInstance"]] = None


class DAQJobStoreTargetInstance(Struct):
    supervisor_id: Optional[str] = None
    is_self: Optional[bool] = None


class DAQJobStoreConfigBase(Struct, kw_only=True):
    remote_topic: Optional[str] = None
    remote_disable: Optional[bool] = None


class DAQJobStoreConfigCSV(DAQJobStoreConfigBase):
    file_path: str
    add_date: bool
    overwrite: Optional[bool] = None


class DAQJobStoreConfigROOT(DAQJobStoreConfigBase):
    file_path: str
    add_date: bool
