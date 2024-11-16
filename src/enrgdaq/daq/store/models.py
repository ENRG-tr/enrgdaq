from typing import Any, Optional

from msgspec import Struct

from enrgdaq.daq.models import DAQJobConfig, DAQJobMessage, DAQRemoteConfig


class DAQJobStoreConfig(Struct, dict=True):
    """
    Used to store the configuration of the DAQ Job Store, usually inside DAQJobConfig.
    """

    csv: "Optional[DAQJobStoreConfigCSV]" = None
    root: "Optional[DAQJobStoreConfigROOT]" = None
    mysql: "Optional[DAQJobStoreConfigMySQL]" = None
    redis: "Optional[DAQJobStoreConfigRedis]" = None

    def has_store_config(self, store_type: Any) -> bool:
        for key in dir(self):
            if key.startswith("_"):
                continue
            value = getattr(self, key)
            if isinstance(value, store_type):
                return True
        return False


class DAQJobMessageStore(DAQJobMessage):
    """
    DAQJobMessageStore is a class that extends DAQJobMessage and is used to store
    configuration and data related to a DAQ (Data Acquisition) job.
    Attributes:
        store_config (DAQJobStoreConfig): Configuration for the DAQ job store.
        keys (list[str]): List of keys associated with the data.
        data (list[list[Any]]): Nested list containing the data.
        tag (str | None): Optional tag associated with the DAQ job.
    """

    store_config: DAQJobStoreConfig
    keys: list[str]
    data: list[list[Any]]
    tag: str | None = None

    def get_remote_config(self) -> Optional[DAQRemoteConfig]:
        """
        Retrieves the remote configuration from the store_config.
        Iterates through the attributes of `self.store_config` to find an instance
        of `DAQJobStoreConfigBase` that has a non-None `remote_config` attribute.
        Returns:
            Optional[DAQRemoteConfig]: The remote configuration if found, otherwise None.
        """

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


class DAQJobStoreConfigBase(Struct, kw_only=True):
    """
    DAQJobStoreConfigBase is a configuration class for DAQ job store,
    that is expected to be extended by specific store configurations, such as CSV, MySQL, etc.

    Attributes:
        remote_config (Optional[DAQRemoteConfig]): Configuration for remote DAQ.
    """

    remote_config: Optional[DAQRemoteConfig] = None


class DAQJobStoreConfigCSV(DAQJobStoreConfigBase):
    file_path: str
    """
    File path to store data in.
    """

    add_date: bool = False
    """
    Include the date in the file path.
    """

    overwrite: bool = False
    """
    Overwrite the file contents always.
    """

    use_gzip: bool = False
    """
    Use gzip compression.
    """


class DAQJobStoreConfigMySQL(DAQJobStoreConfigBase):
    table_name: str


class DAQJobStoreConfigRedis(DAQJobStoreConfigBase):
    key: str
    """
    Redis key to store data in.
    
    Data keys will be prefixed with the redis_key, e.g. for the data key "test", the redis key will be "redis_key.test".
    
    If the expiration is set, the key will be prefixed with the date, e.g. for the data key "test", the redis key will be "redis_key.test:2023-01-01".
    """

    key_expiration_days: Optional[int] = None
    """
    Delete keys older than this number of days.
    
    If None, keys will not be deleted.
    """

    use_timeseries: Optional[bool] = None
    """
    Utilize Redis Timeseries to store data.

    A key called "timestamp" is requires when using timeseries.
    """


class DAQJobStoreConfigROOT(DAQJobStoreConfigBase):
    file_path: str
    add_date: bool
