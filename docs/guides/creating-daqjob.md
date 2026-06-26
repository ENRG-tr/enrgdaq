# Creating a Custom DAQJob

This guide walks through creating a new sensor or data-processing job.
By the end, you will have a working `DAQJob` that reads data and sends
it to your chosen storage backend.

---

## Overview

Every job in ENRGDAQ is a Python class that inherits from `DAQJob` and supervisor auto-discovers it at runtime.

A complete custom job requires:

1. A **config class** (inherits `DAQJobConfig` or `StorableDAQJobConfig`)
2. A **job class** (inherits `DAQJob`) with a `start()` method
3. Preferably, alls to `_put_message_out()` to send data

---

## Step 1: Define the config

Config classes use `msgspec.Struct`. Every field maps to a TOML key.

If your job produces storable data, inherit `StorableDAQJobConfig`
to get the automatic `store_config` field (convention: not needed but do it):

```python
from enrgdaq.daq.store.models import StorableDAQJobConfig

class MySensorConfig(StorableDAQJobConfig):
    """Configuration for my custom sensor."""

    device_path: str = "/dev/sensor0"
    poll_interval_seconds: float = 1.0
    gain: float = 1.0
```

If your job does NOT produce storable data (e.g., a processing step),
inherit plain `DAQJobConfig`:

```python
from enrgdaq.daq.models import DAQJobConfig

class MyFilterConfig(DAQJobConfig):
    threshold: float = 0.5
```

---

## Step 2: Define the job class

Inherit from `DAQJob` and set `config_type` and `config`:

```python
from enrgdaq.daq.base import DAQJob

class DAQJobMySensor(DAQJob):
    config_type = MySensorConfig
    config: MySensorConfig
```

The class name is important — it becomes the `daq_job_type` value in TOML files.
Use the `DAQJob` prefix convention for consistency (e.g., `DAQJobMySensor`).

---

## Step 3: Implement `start()`

The `start()` method is the main loop. Run until `self._has_been_freed`
becomes `True` (the supervisor signals shutdown):

```python
import time

def start(self):
    device = open(self.config.device_path)

    while not self._has_been_freed:
        data = device.read(1024)
        self._put_message_out(
            DAQJobMessageStoreRaw(
                data=data,
                store_config=self.config.store_config,
            )
        )
        time.sleep(self.config.poll_interval_seconds)
```

The `_put_message_out()` method sends a message through the broker. The
message's `store_config` field determines which storage backends receive it.

---

## Step 4: Choose a message type

ENRGDAQ provides several message types. Choose based on your data shape:

| Message type | Use when |
|-------------|----------|
| `DAQJobMessageStoreRaw` | Binary blobs (images, raw waveforms) |
| `DAQJobMessageStoreTabular` | **Deprecated, don't use** |
| `DAQJobMessageStorePyArrow` | Columnar numerical data (high performance) |

### Binary data (images, waveforms)

```python
from enrgdaq.daq.store.models import DAQJobMessageStoreRaw

self._put_message_out(
    DAQJobMessageStoreRaw(
        data=image_bytes,
        store_config=self.config.store_config,
    )
)
```

### Columnar data (high performance)

```python
import pyarrow as pa
from enrgdaq.daq.store.models import DAQJobMessageStorePyArrow

self._put_message_out(
    DAQJobMessageStorePyArrow(
        store_config=self.config.store_config,
        table=pa.table({
            "timestamp": [time.time()],
            "voltage": [42.0],
            "current": [0.1],
        }),
    ),
)
```

---

## Step 5: Use in a config file

Save as `configs/my_sensor.toml`:

```toml
daq_job_type = "DAQJobMySensor"
device_path = "/dev/sensor0"
poll_interval_seconds = 0.5
gain = 2.0
verbosity = "DEBUG"

[store_config.csv]
file_path = "sensor_data.csv"
add_date = true

[store_config.raw]
file_path = "sensor_raw.bin"
add_date = true
```

Run:

```bash
uv run python src/run.py --daq-job-config-path configs/
```

---

## Subscribing to other jobs

If your job needs to **consume** messages from other jobs
set `topics_to_subscribe` or inherit `allowed_message_in_types`:

```python
class DAQJobMyFilter(DAQJob):
    config_type = MyFilterConfig
    config: MyFilterConfig

    # Only accept these message types
    allowed_message_in_types = [DAQJobMessageStorePyArrow]

    def handle_message(self, message: DAQJobMessageStorePyArrow) -> bool:
        # Process incoming data
        table = message.get_table()
        # ... transform and re-emit ...
        return True
```

The `handle_message()` method is called for every incoming message that
passes the type filter. Return `True` if you handled it.

---

## Next steps

- [Creating a Store Backend](creating-store-backend.md) — add a new storage format
- [Configuration](../getting-started/configuration.md) — all config options
