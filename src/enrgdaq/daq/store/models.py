from collections import defaultdict
from functools import cache
from typing import Optional

import pyarrow as pa
from msgspec import Struct, field
from numpy import ndarray

from enrgdaq.daq.models import (
    DAQJobConfig,
    DAQJobMessage,
    DAQRemoteConfig,
    SHMHandle,
)


@cache
def _get_store_config_base_to_store_job_mapping():
    """
    Returns a mapping of store config base types to store job types,
    used to route messages to the correct store job.
    """
    from enrgdaq.daq.store.base import DAQJobStore
    from enrgdaq.daq.types import get_all_daq_job_types

    res = defaultdict(list)
    store_jobs = [
        job
        for job in get_all_daq_job_types()
        if issubclass(job, DAQJobStore) and job is not DAQJobStore
    ]

    for store_job in store_jobs:
        for store_config_type in store_job.allowed_store_config_types:
            res[store_config_type].append(store_job)
    return res


class DAQJobStoreConfig(Struct, dict=True):
    """
    Used to store the configuration of the DAQ Job Store, usually inside DAQJobConfig.
    """

    csv: "Optional[DAQJobStoreConfigCSV]" = None
    root: "Optional[DAQJobStoreConfigROOT]" = None
    hdf5: "Optional[DAQJobStoreConfigHDF5]" = None
    mysql: "Optional[DAQJobStoreConfigMySQL]" = None
    redis: "Optional[DAQJobStoreConfigRedis]" = None
    raw: "Optional[DAQJobStoreConfigRaw]" = None
    memory: "Optional[DAQJobStoreConfigMemory]" = None

    store_types: set[type["DAQJobStoreConfigBase"]] = field(default_factory=set)

    def __post_init__(self):
        for key in dir(self):
            value = getattr(self, key)
            if isinstance(value, DAQJobStoreConfigBase):
                self.store_types.add(type(value))

    def has_store_config(self, store_type: type["DAQJobStoreConfigBase"]) -> bool:
        return store_type in self.store_types


class DAQJobMessageStore(DAQJobMessage):
    """
    DAQJobMessageStore is a class that extends DAQJobMessage and is used to store
    configuration and data related to a DAQ (Data Acquisition) job.
    Attributes:
        store_config (DAQJobStoreConfig): Configuration for the DAQ job store.
        tag (str | None): Optional tag associated with the message.
    """

    store_config: DAQJobStoreConfig
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

    def __post_init__(self):
        mappings = _get_store_config_base_to_store_job_mapping()
        for store_type in self.store_config.store_types:
            for store_job in mappings[store_type]:
                self.route_keys.add(store_job.__name__)

        remote_config = self.get_remote_config()
        if not remote_config or not remote_config.remote_disable:
            self.route_keys.add("DAQJobRemote")


class DAQJobMessageStoreSHM(DAQJobMessageStore, kw_only=True):
    shm: SHMHandle


class DAQJobMessageStoreTabular(DAQJobMessageStore, kw_only=True):
    """
    DAQJobMessageStoreTabular is a class that inherits from DAQJobMessageStore and represents a tabular data store for DAQ job messages.
    Attributes:
        keys (list[str]): A list of strings representing the keys or column names of the tabular data.
        data (list[list[str | float | int]]): A list of lists where each inner list represents a row of data..
    """

    keys: list[str]
    data: Optional[list[list[str | float | int] | ndarray]] = None
    data_columns: Optional[dict[str, list[str | float | int] | ndarray]] = None


class DAQJobMessageStorePyArrow(DAQJobMessageStore, kw_only=True):
    """
    DAQJobMessageStorePyArrow is a high-performance message store using PyArrow's
    columnar format. Optimized for numerical data with zero-copy reads.

    Attributes:
        table (pa.Table): A PyArrow Table containing the columnar data.
    """

    table: pa.Table


class DAQJobMessageStoreRaw(DAQJobMessageStore, kw_only=True):
    """
    DAQJobMessageStoreRaw is a class that inherits from DAQJobMessageStore and represents
    a raw data message store for DAQ jobs.
    Attributes:
        data (bytes): The raw data associated with the DAQ job message.
    """

    data: bytes


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
    Overwrite the file contents.
    """

    use_gzip: bool = False
    """
    Use gzip compression.
    """


class DAQJobStoreConfigRaw(DAQJobStoreConfigBase):
    file_path: str
    """
    File path to store data in.
    """

    add_date: bool = False
    """
    Overwrite the file contents always.
    """

    overwrite: bool = True
    """
    Overwrite the file contents.
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
    tree_name: str
    compression_type: str = "ZSTD"
    compression_level: int = 5


class DAQJobStoreConfigHDF5(DAQJobStoreConfigBase):
    file_path: str
    add_date: bool
    dataset_name: str


class DAQJobStoreConfigMemory(DAQJobStoreConfigBase):
    """
    Configuration for in-memory DAQ job store.
    """

    pass
