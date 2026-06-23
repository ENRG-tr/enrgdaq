# ENRGDAQ

Data acquisition framework for neutrino physics experiments.
Handles readout, processing, and storage at multi-Gbps rates.

## Quick start

```bash
git clone https://github.com/ENRG-tr/enrgdaq.git
cd enrgdaq
uv sync
uv run python src/run.py --daq-job-config-path configs/examples/
```

## How it works

Everything runs as independent OS processes called DAQJobs. A supervisor
spawns them from TOML config files, monitors their health, and restarts
them if they crash. Jobs communicate through a ZMQ pub/sub broker inside
the supervisor. For bulk data like waveforms and images, the producer
writes directly to a shared memory ring buffer and sends a tiny handle
over ZMQ. The consumer reads the data with zero copies.

## Storage backends

CSV, ROOT, HDF5, MySQL, Redis, Raw, Memory. Mix multiple backends on the
same job by stacking `[store_config.*]` sections in the TOML file.

## Custom jobs

Subclass `DAQJob`, define a config, implement `start()`. The system
auto-discovers your class anywhere under `src/enrgdaq/`.

```python
from enrgdaq.daq.base import DAQJob

class DAQJobMySensor(DAQJob):
    config_type = MySensorConfig

    def start(self):
        while not self._has_been_freed:
            data = read_hardware()
            self._put_message_out(DAQJobMessageStoreRaw(
                data=data, store_config=self.config.store_config
            ))
```

## Commands

- `uv run python src/run.py` starts the supervisor
- `uv run pytest src/tests/ -v` runs the test suite
- `uv run ruff check src/` lints the code

## Platform notes

Linux and macOS are fully supported.
Windows support is partial (no fork, no shared memory ring buffers).

