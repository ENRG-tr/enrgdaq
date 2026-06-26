# Creating a Storage Backend

This guide shows how to add a new storage format to ENRGDAQ.
Store backends are specialized DAQJobs that receive data from
producers and flush it to disk, database, or memory.

---

## How stores work

Every store backend inherits from `DAQJobStore` and follows this pattern:

1. Subscribes to its own topic automatically
2. Accumulates incoming messages in a queue
3. Periodically writes buffered data to the destination

The system routes data to stores automatically. When a producer sets
`store_config.my_new_store`, messages are routed to your store with
no manual topic configuration.

---

## Step 1: Define the store config class

Create a config class inheriting `DAQJobStoreConfigBase`. These fields
appear in the producer's TOML under `[store_config.my_new_store]`:

```python
from enrgdaq.daq.store.models import DAQJobStoreConfigBase

class DAQJobStoreConfigJSON(DAQJobStoreConfigBase):
    file_path: str        # Output file path
    add_date: bool = True  # Include date in filename
    indent: int = 2       # JSON indentation
```

When a producer config includes this section, it routes messages to your store:

```toml
[store_config.my_new_store]
file_path = "data.json"
```

The mapping is automatic because of the config field name on `DAQJobStoreConfig`.
Add your field name to `DAQJobStoreConfig`:

```python
# enrgdaq/daq/store/models.py
class DAQJobStoreConfig(Struct, dict=True):
    csv: Optional[DAQJobStoreConfigCSV] = None
    # ... existing fields ...
    json: Optional[DAQJobStoreConfigJSON] = None  # ADD THIS
```

---

## Step 2: Define the store job class

Inherit from `DAQJobStore` and declare what config types and message types
it handles:

```python
from enrgdaq.daq.store.base import DAQJobStore

class DAQJobStoreJSON(DAQJobStore):
    config_type = DAQJobStoreJSONConfig  # Own job config
    allowed_store_config_types = [DAQJobStoreConfigJSON]  # Producer config types
    allowed_message_in_types = [DAQJobMessageStore]  # Message types to accept
```

- **`config_type`** — the config for this store job itself (e.g., output directory)
- **`allowed_store_config_types`** — which `[store_config.*]` sections trigger routing to this store
- **`allowed_message_in_types`** — which message types to accept

---

## Step 3: Implement `handle_message()`

Called for every incoming message. Buffer the data:

```python
from collections import deque
from dataclasses import dataclass
from datetime import datetime

@dataclass
class JSONFile:
    file: Any
    last_flush_date: datetime
    write_queue: deque

class DAQJobStoreJSON(DAQJobStore):
    # ... (config and type declarations) ...

    def __init__(self, config, **kwargs):
        super().__init__(config, **kwargs)
        self._open_files: dict[str, JSONFile] = {}

    def handle_message(self, message: DAQJobMessageStore) -> bool:
        # Let the base class validate the message type
        if not super().handle_message(message):
            return False

        # Get the store-specific config from the message
        json_config = message.store_config.json
        file_path = json_config.file_path

        # Open file on first use
        if file_path not in self._open_files:
            self._open_files[file_path] = JSONFile(
                file=open(file_path, "a"),
                last_flush_date=datetime.now(),
                write_queue=deque(),
            )

        # Buffer the data
        if isinstance(message, DAQJobMessageStorePyArrow):
            table = message.get_table()
            # Convert PyArrow table to rows
            for row in table.to_pylist():
                self._open_files[file_path].write_queue.append(row)
            message.release()
        elif isinstance(message, DAQJobMessageStoreTabular):
            for row in message.data:
                self._open_files[file_path].write_queue.append(row)

        return True
```

---

## Step 4: Implement `store_loop()`

Called in a loop by `DAQJobStore.start()`. Write buffered data and flush:

```python
import json

JSON_FLUSH_INTERVAL_SECONDS = 5

def store_loop(self):
    for file_path, json_file in list(self._open_files.items()):
        if json_file.file.closed:
            del self._open_files[file_path]
            continue

        # Write all buffered rows
        while json_file.write_queue:
            row = json_file.write_queue.popleft()
            json_file.file.write(json.dumps(row, indent=2) + "\n")

        # Flush periodically
        elapsed = (datetime.now() - json_file.last_flush_date).total_seconds()
        if elapsed >= JSON_FLUSH_INTERVAL_SECONDS:
            json_file.file.flush()
            json_file.last_flush_date = datetime.now()
```

---

## Step 5: Define the store job config

Create the config class for running the store as a standalone job:

```python
from enrgdaq.daq.models import DAQJobConfig

class DAQJobStoreJSONConfig(DAQJobConfig):
    out_dir: str = "out/"
```

This is used in the store's own TOML file:

```toml
# configs/store_json.toml
daq_job_type = "DAQJobStoreJSON"
out_dir = "out/"
```

---

## Step 6: Register on `DAQJobStoreConfig`

Update `DAQJobStoreConfig` in `enrgdaq/daq/store/models.py`:

```python
class DAQJobStoreConfig(Struct, dict=True):
    csv: Optional[DAQJobStoreConfigCSV] = None
    root: Optional[DAQJobStoreConfigROOT] = None
    hdf5: Optional[DAQJobStoreConfigHDF5] = None
    mysql: Optional[DAQJobStoreConfigMySQL] = None
    redis: Optional[DAQJobStoreConfigRedis] = None
    raw: Optional[DAQJobStoreConfigRaw] = None
    memory: Optional[DAQJobStoreConfigMemory] = None
    json: Optional[DAQJobStoreConfigJSON] = None  # New field
```

The `__post_init__` method automatically discovers instances of `DAQJobStoreConfigBase`

---

## Complete example: JSON store

Here's a minimal, functional JSON store in one file:

```python
import json
import os
from collections import deque
from dataclasses import dataclass
from datetime import datetime

from enrgdaq.daq.models import DAQJobConfig
from enrgdaq.daq.store.base import DAQJobStore
from enrgdaq.daq.store.models import (
    DAQJobMessageStore,
    DAQJobMessageStoreTabular,
    DAQJobStoreConfigBase,
)


class DAQJobStoreConfigJSON(DAQJobStoreConfigBase):
    file_path: str
    add_date: bool = True


class DAQJobStoreJSONConfig(DAQJobConfig):
    out_dir: str = "out/"


@dataclass
class _JSONFile:
    file: object
    write_queue: deque
    last_flush: datetime


class DAQJobStoreJSON(DAQJobStore):
    config_type = DAQJobStoreJSONConfig
    allowed_store_config_types = [DAQJobStoreConfigJSON]
    allowed_message_in_types = [DAQJobMessageStore]

    def __init__(self, config, **kwargs):
        super().__init__(config, **kwargs)
        self._files: dict[str, _JSONFile] = {}

    def handle_message(self, message):
        if not super().handle_message(message):
            return False
        path = os.path.join(self.config.out_dir, message.store_config.json.file_path)
        if path not in self._files:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            self._files[path] = _JSONFile(open(path, "a"), deque(), datetime.now())
        if isinstance(message, DAQJobMessageStoreTabular):
            for row in message.data:
                self._files[path].write_queue.append(row)
        return True

    def store_loop(self):
        for path, f in list(self._files.items()):
            while f.write_queue:
                f.file.write(json.dumps(f.write_queue.popleft()) + "\n")
            if (datetime.now() - f.last_flush).total_seconds() > 5:
                f.file.flush()
                f.last_flush = datetime.now()

    def __del__(self):
        for f in self._files.values():
            if not f.file.closed:
                f.file.close()
        super().__del__()
```

---

## Next steps

- [Creating a DAQJob](creating-daqjob.md) — build a sensor job that writes to your store
- [Storage Backends Reference](../reference/storage-backends.md) — comparison of all backends
