from typing import Any, Optional

from msgspec import Struct

from daq.models import DAQJobConfig, DAQJobMessage, DAQRemoteConfig


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

    def get_remote_config(self) -> Optional[DAQRemoteConfig]:
        for key in dir(self.store_config):
            value = getattr(self.store_config, key)
            if not isinstance(value, DAQJobStoreConfigBase):
                continue
            if value.remote_config is None:
                continue
            return value.remote_config
        return None


class StorableDAQJobConfig(DAQJobConfig):
    store_config: DAQJobStoreConfig


class DAQJobStoreTarget(Struct):
    instances: Optional[list["DAQJobStoreTargetInstance"]] = None


class DAQJobStoreTargetInstance(Struct):
    supervisor_id: Optional[str] = None
    is_self: Optional[bool] = None


class DAQJobStoreConfigBase(Struct, kw_only=True):
    remote_config: Optional[DAQRemoteConfig] = None


class DAQJobStoreConfigCSV(DAQJobStoreConfigBase):
    file_path: str
    add_date: bool
    overwrite: Optional[bool] = None


class DAQJobStoreConfigROOT(DAQJobStoreConfigBase):
    file_path: str
    add_date: bool
