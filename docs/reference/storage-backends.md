# Storage Backends

ENRGDAQ supports seven storage backends out of the box. Each is a
separate DAQJob process that subscribes to store topics and flushes
data to its destination.

---

## Backend comparison

| Backend | Config key | Format | Best for |
|---------|-----------|--------|----------|
| CSV | `store_config.csv` | Text (comma-separated) | Quick inspection, spreadsheets, lightweight logging |
| ROOT | `store_config.root` | Binary (CERN ROOT) | High-energy physics analysis, TTree-based workflows |
| HDF5 | `store_config.hdf5` | Binary (HDF5) | Large numerical datasets, hierarchical organization |
| MySQL | `store_config.mysql` | Relational database | Long-term storage, SQL queries, web dashboards |
| Redis | `store_config.redis` | In-memory key-value | Real-time dashboards, time-series, caching |
| Raw | `store_config.raw` | Binary (raw bytes) | Images, waveforms, binary blobs |
| Memory | `store_config.memory` | In-process dict | Testing, in-memory buffering, no disk I/O |

---

## CSV Store

Writes tabular data to CSV files. Supports PyArrow native CSV writing
(fast path) and Python csv.writer (slow path for tabular messages).

### Performance notes

- PyArrow messages use `pa_csv.write_csv()` directly — columnar, no row iteration
- Tabular messages use Python's `csv.writer` — slower, but flexible
- zstd streaming compression available (`use_zstd = true`)

### Features

- Date-appended filenames (`data_2026-01-01.csv`)
- Append or overwrite modes
- zstd frame-level compression for crash-safe streaming
- Auto-creates output directories

---

## ROOT Store

Writes data as ROOT TTrees using `uproot`. The standard format in
high-energy and nuclear physics.

### Configuration

```toml
[store_config.root]
file_path = "events.root"
add_date = true
tree_name = "Events"
compression_type = "ZSTD"     # ZLIB, LZMA, LZ4, ZSTD
compression_level = 5
```

### Features

- Compatible with CERN ROOT analysis frameworks
- ZSTD compression with configurable level
- Branch-per-key in the TTree

---

## HDF5 Store

Writes to HDF5 files using `h5py`. Ideal for large structured datasets
with hierarchical grouping.

### Configuration

```toml
[store_config.hdf5]
file_path = "data.h5"
add_date = true
dataset_name = "events"
```

### Features

- Date-appended filenames
- Named datasets within the HDF5 file
- Compatible with pandas `read_hdf()`, h5py, PyTables

---

## MySQL Store

Writes data to a MySQL/MariaDB database. Useful for long-term storage
with SQL querying.

### Configuration

```toml
[store_config.mysql]
table_name = "sensor_data"
```

Connection parameters are read from environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MYSQL_HOST` | `localhost` | Database host |
| `MYSQL_PORT` | `3306` | Database port |
| `MYSQL_USER` | `root` | Database user |
| `MYSQL_PASSWORD` | (empty) | Database password |
| `MYSQL_DB` | `enrgdaq` | Database name |

### Features

- Auto-creates table from message keys (if not exists)
- Column type inference from data
- Batch inserts for performance

---

## Redis Store

Writes to Redis, with optional RedisTimeSeries support for real-time
monitoring dashboards.

### Configuration

```toml
[store_config.redis]
key = "daq:sensor1"
key_expiration_days = 30
use_timeseries = true
```

### Features

- Key prefixing (`daq:sensor1.temperature`)
- Optional TTL (auto-delete old keys)
- RedisTimeSeries integration for Grafana dashboards
- Pipeline batching for throughput

---

## Raw Store

Writes raw bytes directly to files. Used for binary data like camera
images and digitizer waveforms.

### Configuration

```toml
[store_config.raw]
file_path = "image.jpg"
add_date = true
overwrite = true
```

### Features

- Append or overwrite modes
- Date-appended filenames
- No formatting overhead — bytes go directly to disk

---

## Memory Store

Stores data in a Python dictionary in the store process's memory.
Useful for testing, in-memory buffering, or when disk I/O is not needed.

### Configuration

```toml
[store_config.memory]
# No required fields — store everything in memory
```

### Features

- Zero I/O — all data stays in RAM
- Accessible via the store job's API
- Optional disposal after N entries (`dispose_after_n_entries`)
- Optional void mode (`void_data = true`) for throughput testing

---

## Choosing a backend

| Scenario | Recommended backend |
|----------|-------------------|
| Quick inspection during development | CSV |
| Physics analysis (CERN ROOT ecosystem) | ROOT |
| Large-scale numerical analysis (Python) | HDF5 |
| Long-term catalog with SQL queries | MySQL |
| Real-time dashboards (Grafana) | Redis |
| Camera images, raw waveforms | Raw |
| Unit testing, buffering | Memory |

You can use **multiple backends simultaneously** for the same data.
A single DAQJob can write to CSV for inspection AND ROOT for analysis:

```toml
[store_config.csv]
file_path = "data.csv"

[store_config.root]
file_path = "data.root"
add_date = true
tree_name = "Events"
```

---

## Next steps

- [Creating a Store Backend](../guides/creating-store-backend.md) — build a custom store
- [Message Flow](message-flow.md) — how messages reach stores
