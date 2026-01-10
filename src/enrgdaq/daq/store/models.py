from collections import defaultdict
from functools import cache
from typing import Optional

import pyarrow as pa
from msgspec import Struct, field
from numpy import ndarray

from enrgdaq.daq.models import (
    DAQJobConfig,
    DAQJobMessage,
    RingBufferHandle,
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
        target_local_supervisor (bool): Whether to send the message to store job topics of the local supervisor.
    """

    store_config: DAQJobStoreConfig
    tag: str | None = None
    target_local_supervisor: bool = False

    def __post_init__(self):
        from enrgdaq.daq.topics import Topic

        mappings = _get_store_config_base_to_store_job_mapping()
        for store_type in self.store_config.store_types:
            for store_job in mappings[store_type]:
                if self.target_local_supervisor:
                    self.topics.add(
                        Topic.store_supervisor(self.supervisor_id, store_job.__name__)
                    )
                else:
                    self.topics.add(Topic.store(store_job.__name__))


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
        table (pa.Table | None): A PyArrow Table containing the columnar data.
            None when using zero-copy mode (handle is set instead).
        handle (RingBufferHandle | None): Handle to PyArrow data in shared memory.
            When set, the table is loaded from shared memory using zero-copy.
    """

    table: pa.Table | None = None
    handle: RingBufferHandle | None = None

    def get_table(self) -> pa.Table:
        """
        Get the PyArrow table, loading from shared memory if using zero-copy.

        Returns:
            pa.Table: The PyArrow table.
        """
        if self.handle is not None:
            return self.handle.load_pyarrow()
        if self.table is not None:
            return self.table
        raise ValueError("Neither table nor handle is set")

    def release(self):
        """
        Release the ring buffer slot back to the pool (if using zero-copy).

        Call this after you're done with the table to allow the slot to be reused.
        No-op if not using zero-copy mode.
        """
        if self.handle is not None:
            self.handle.release()

    def __getstate__(self):
        state = self.__dict__.copy()
        # If we have a handle, we serialize it (efficient local transfer)
        # The supervisor will convert this to full data if sending remotely
        if self.handle is not None:
            # Table shouldn't be serialized if we're sending the handle
            state["table"] = None
        elif state.get("table") is not None:
            # Convert table to bytes using Arrow IPC
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, state["table"].schema) as writer:
                writer.write_table(state["table"])
            state["table"] = sink.getvalue().to_pybytes()
        return state

    def __setstate__(self, state):
        # Restore table from bytes
        table_bytes = state.get("table")
        if table_bytes is not None and isinstance(table_bytes, bytes):
            with pa.ipc.open_stream(table_bytes) as reader:
                state["table"] = reader.read_all()
        state["handle"] = None  # Handle is never deserialized
        self.__dict__.update(state)


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
    """

    pass


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

    key_expiration_days: int | None = None
    """
    Delete keys older than this number of days.
    
    If None, keys will not be deleted.
    """

    use_timeseries: bool | None = None
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
