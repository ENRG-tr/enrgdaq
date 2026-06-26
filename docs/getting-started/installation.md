# Installation

This guide covers installing ENRGDAQ on macOS and Linux.
Windows has limited support — see the [platform notes](#platform-notes) below.

---

## Requirements

- **Python 3.12+** — ENRGDAQ uses modern type hints and `msgspec` features
- **uv**
- **Git** — to clone the repository

---

## Install with uv (recommended)

```bash
# Clone the repository
git clone https://github.com/ENRG-tr/enrgdaq.git
cd enrgdaq

# Install all dependencies in a virtual environment
uv sync
```

Verify the installation:

```bash
uv run python -c "import enrgdaq; print('ENRGDAQ ready')"
```

---

## Platform notes

| Platform | Status    | Notes                                                       |
| -------- | --------- | ----------------------------------------------------------- |
| Linux    | Supported | Full feature set including shared memory ring buffers       |
| macOS    | Supported | Full feature set including shared memory ring buffers       |
| Windows  | Partial   | No `fork()`, no IPC sockets, no shared memory ring buffers  |

On Windows, ENRGDAQ falls back to in-process threading for DAQJobs and skips
zero-copy shared memory. Throughput will be lower than on Linux/macOS.

---

## Optional dependencies

Some DAQJob types require optional hardware SDKs:

- **CAEN digitizer & HV** — requires `caen-libs` (included in `requirements.txt`)
- **Xiaomi Mijia BLE sensor** — requires `lywsd03mmc-client` (installed via git)
- **N1081B SDK** — requires `n1081b-sdk` (installed via git)

These are included when you install with `uv sync`.
If a hardware library is unavailable, the correspondingD AQJob logs a warning and
disables itself at runtime.

---

## Development setup

If you plan to contribute to ENRGDAQ:

```bash
# Install development dependencies (linter, pre-commit hooks)
uv sync --group dev

# Set up pre-commit hooks (lint checks before every commit)
uv run pre-commit install

# Run the test suite
uv run pytest src/tests/ -v
```

---

## Next steps

- [Quickstart](quickstart.md) — run your first DAQJob in 5 minutes
- [Configuration](configuration.md) — understand the TOML config system
