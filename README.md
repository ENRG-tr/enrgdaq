# ENRGDAQ

**ENRGDAQ** is a modular framework for physics experiments, specifically neutrino detection. It handles data collection, processing, and storage from various hardware sensors and devices.

## Core Features

- **Job-based modularity**: Each "DAQJob" runs as an independent process.
- **ZMQ messaging**: High-throughput inter-process communication using a pub/sub broker with topic-based routing.
- **Zero-copy transfer**: Shared memory ring buffers powered by PyArrow (fallback to Python's SharedMemory when PyArrow is not used) for Gbps-level throughput.
- **Supervision**: Automatic process monitoring and failure recovery.
- **Real-time metrics**: Built-in tracking for message counts, throughput, and latencies.
- **Distributed scaling**: Support for cross-node communication in multi-machine setups.
- **TOML configuration**: System behavior is defined via TOML files, requiring no code changes for deployment.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         SUPERVISOR                              │
│  - Manages DAQJob lifecycle (start, stop, restart)              │
│  - Initializes ZMQ message broker                               │
│  - Collects and reports statistics                              │
└─────────────────────────────────────────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
   ┌─────────┐        ┌─────────┐        ┌─────────┐
   │ DAQJob  │   pub  │ DAQJob  │   pub  │ DAQJob  │
   │ Sensor  │ ──────►│ Store   │ ◄──────│ Stats   │
   └─────────┘  topic └─────────┘  topic └─────────┘

```

### Message Flow

Inter-process communication relies on a ZMQ pub/sub broker:

1. **Producers** (sensors or data sources) publish messages to specific topics.
2. **Consumers** (storage backends or handlers) subscribe to those topics.
3. **Routing** is handled via the `topics` attribute (e.g., `store.DAQJobStoreCSV`).

### Storage Backends

| Backend | Description                                  |
| ------- | -------------------------------------------- |
| CSV     | Tabular data written to CSV files            |
| ROOT    | High-energy physics ROOT format              |
| HDF5    | Hierarchical Data Format                     |
| MySQL   | Relational database storage                  |
| Redis   | In-memory data store with timeseries support |
| Raw     | Direct binary file output                    |

---

## Installation

### Requirements

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) (recommended) or pip

### Setup

```bash
# Clone the repository
git clone https://github.com/ENRG-tr/enrgdaq.git
cd enrgdaq

# Install dependencies with uv
uv sync

# Or use pip
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

```

---

## Usage

### 1. Configuration

Define your setup in `configs/`. You can find templates in `configs/examples/`.

**Example: Simple Sensor Job**

```toml
# configs/my_sensor.toml
daq_job_type = "DAQJobMySensor"
interval_seconds = 1.0

[store_config.csv]
file_path = "sensor_data.csv"
add_date = true

```

### 2. Execution

```bash
# Start the supervisor with default configs
uv run python src/main.py

# Specify a custom config directory
uv run python src/main.py --daq-job-config-path configs/my_project

```

### 3. Monitoring

The system automatically generates two statistics files:

- `stats.csv`: Per-job metrics (counts and latencies).
- `stats_remote.csv`: Aggregated supervisor metrics (MB/s throughput).

---

## Creating Custom DAQJobs

Inherit from the `DAQJob` base class to create custom logic.

```python
from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import DAQJobConfig
from enrgdaq.daq.store.models import DAQJobMessageStoreTabular, DAQJobStoreConfig

class MyJobConfig(DAQJobConfig):
    my_setting: str
    interval_seconds: float = 1.0

class DAQJobMyJob(DAQJob):
    config_type = MyJobConfig
    config: MyJobConfig

    def start(self):
        while True:
            # Produce and route data
            self._put_message_out(
                DAQJobMessageStoreTabular(
                    store_config=DAQJobStoreConfig(csv=...),
                    keys=["timestamp", "value"],
                    data=[[time.time(), 42.0]],
                )
            )
            time.sleep(self.config.interval_seconds)

```

---

## Development

### Maintenance Tasks

- **Pre-commit hooks**: `pre-commit install`
- **Testing**: `uv run python -m pytest src/tests/ -v`
- **Linting**: `uv run ruff check src/`

### Project Structure

- `src/enrgdaq/supervisor.py`: Core process management.
- `src/enrgdaq/daq/base.py`: Base classes for jobs.
- `src/enrgdaq/utils/shared_ring_buffer.py`: High-performance memory management.
- `configs/`: Deployment configurations.

### Platform Support

| Platform | Status    | Notes                                                    |
| -------- | --------- | -------------------------------------------------------- |
| Linux    | Supported | ✅ Full feature set available                            |
| macOS    | Supported | ✅ Full feature set available                            |
| Windows  | Partial   | ⚠️ No fork(), IPC sockets, or shared memory ring buffers |
