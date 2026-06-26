# Configuration

ENRGDAQ is configured entirely through TOML files. No code changes are required to
deploy an experiment. You just define jobs, storage, and system behavior in config files.

---

## How configuration works

The supervisor scans a directory for `.toml` files. Each file defines one DAQJob.
The `daq_job_type` field in each file maps to a Python class:

Under the hood, TOML sections become nested `msgspec.Struct` fields. A job
author defines a config class that inherits `DAQJobConfig`, and TOML keys
map directly to its fields.

---

## Supervisor-level config

The supervisor itself also has a config file at `supervisor.toml`.

```toml
[info]
supervisor_id = "lab-server-1"
supervisor_tags = ["production", "wcd"]

[federation]
is_server = true
server_xpub_url = "tcp://*:16382"
server_xsub_url = "tcp://*:16383"

[cnc]
is_server = true
rest_api_enabled = true
rest_api_host = "0.0.0.0"
rest_api_port = 8000

ring_buffer_size_mb = 512
ring_buffer_slot_size_kb = 1024
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `info.supervisor_id` | string | (required) | Unique name for this supervisor node |
| `info.supervisor_tags` | string[] | `[]` | Free-form tags for filtering |
| `federation.is_server` | bool | `false` | Accept connections from other supervisors |
| `federation.server_xpub_url` | string | — | XPUB endpoint to expose (server mode) |
| `federation.server_xsub_url` | string | — | XSUB endpoint to expose (server mode) |
| `cnc.is_server` | bool | `false` | Run the CNC command server |
| `cnc.rest_api_enabled` | bool | `false` | Enable the CNC REST API |
| `cnc.rest_api_host` | string | `"localhost"` | REST API bind address |
| `cnc.rest_api_port` | int | `8000` | REST API port |
| `ring_buffer_size_mb` | int | `256` | Shared memory ring buffer size in MB |
| `ring_buffer_slot_size_kb` | int | `1024` | Size of each ring buffer slot in KB |

---

## DAQJob config (per-job)

Every job config TOML file must include at minimum:

```toml
daq_job_type = "DAQJobTest"
```

This tells the supervisor which Python class to instantiate. Beyond that,
each job type defines its own config fields.

### Common fields (inherited from `DAQJobConfig`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `daq_job_type` | string | (required) | Python class name of the DAQJob |
| `verbosity` | string | `"INFO"` | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `daq_job_unique_id` | string | auto | Unique identifier (auto-generated if absent) |
| `use_shm_when_possible` | bool | `true` | Use shared memory for message transfer |
| `topics_to_subscribe` | string[] | `[]` | Extra ZMQ topics to subscribe to |

---

## Store configuration

Any job that produces data can include a `[store_config]` section. The system
automatically routes data to the matching store backend.

```toml
daq_job_type = "DAQJobMySensor"

[store_config.csv]
file_path = "sensor_readings.csv"
add_date = true

[store_config.hdf5]
file_path = "sensor_readings.h5"
add_date = true
dataset_name = "events"
```

A single job can write to **multiple stores simultaneously**. Just add more
`[store_config.*]` sections.

Also note that `store_config` is a convention: there are some jobs that use store_config alongside with e.g. waveform_store_config.

### Available store backends

| Backend | Config key | Best for |
|---------|-----------|----------|
| CSV | `store_config.csv` | Tabular data, human-readable, quick inspection |
| ROOT | `store_config.root` | High-energy physics analysis (CERN ecosystem) |
| HDF5 | `store_config.hdf5` | Large numerical datasets, hierarchical data |
| MySQL | `store_config.mysql` | Relational queries, web dashboards |
| Redis | `store_config.redis` | Real-time time-series, in-memory caching |
| Raw | `store_config.raw` | Binary blobs (camera images, raw waveforms) |
| Memory | `store_config.memory` | In-process buffering, no disk I/O |

### CSV store options

```toml
[store_config.csv]
file_path = "data.csv"
add_date = true          # Append date to filename (e.g., data_2026-01-01.csv)
overwrite = false        # Append to file instead of overwriting
use_zstd = false         # Enable zstd streaming compression
```

### HDF5 store options

```toml
[store_config.hdf5]
file_path = "data.h5"
add_date = true
dataset_name = "events"  # HDF5 dataset name
```

### ROOT store options

```toml
[store_config.root]
file_path = "data.root"
add_date = true
tree_name = "events"
compression_type = "ZSTD"
compression_level = 5
```

### MySQL store options

```toml
[store_config.mysql]
table_name = "sensor_data"
```

MySQL connection parameters are read from environment variables or a local config.
By default, the MySQL store connects to `localhost` with no password.

### Redis store options

```toml
[store_config.redis]
key = "daq:sensor1"
key_expiration_days = 30  # Auto-delete keys older than 30 days
use_timeseries = true     # Use RedisTimeSeries module
```

### Raw store options

```toml
[store_config.raw]
file_path = "image.jpg"
add_date = true
overwrite = true          # Always overwrite (default for raw)
```

---

## Example: complete camera config

```toml
daq_job_type = "DAQJobCamera"
camera_device_index = 0
store_interval_seconds = 5
verbosity = "INFO"

[store_config.raw]
file_path = "camera_capture.jpg"
overwrite = true

[store_config.csv]
file_path = "movement_detection.csv"
add_date = true
```

This config:

- Opens camera index 0
- Saves a raw image every 5 seconds
- Logs movement detection events to a daily CSV

---

## Config discovery

The supervisor scans the config directory recursively. Each `.toml` file becomes
one DAQJob. You can organize configs however you like:

```
configs/run_42/
  supervisor.toml
  sensors/
    digitizer.toml
    camera.toml
  storage/
    csv_store.toml
    root_store.toml
```

---

## Next steps

- [Creating a DAQJob](../guides/creating-daqjob.md) — define custom config fields for your job
- [Deployment](../guides/deployment.md) — multi-machine federation config
