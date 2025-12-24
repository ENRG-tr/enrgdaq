# ENRGDAQ System Summary

ENRGDAQ is a high-performance, distributed Data Acquisition (DAQ) system designed for low-latency and high-throughput data processing, typically used in physics experiments or hardware monitoring.

## System Architecture

The system is built on a multi-process architecture where a **Supervisor** manages multiple **DAQ Jobs**. Communication is handled via a flexible messaging system that supports both local inter-process communication (IPC) and remote network communication.

### Core Components

1.  **Supervisor**:

    - The orchestrator of the system.
    - Manages the lifecycle of DAQ Job processes.
    - Handles message routing between jobs.
    - Generates dynamic route mappings to allow jobs to communicate directly via queues, bypassing the supervisor for high-volume data.
    - Provides real-time statistics on throughput and health.

2.  **DAQ Job**:

    - The base unit of work. Every instrument driver, data store, or processing module is a `DAQJob`.
    - Runs in its own process to ensure isolation and leverage multi-core performance.

3.  **Messaging System**:

    - Uses `msgspec` for extremely fast serialization.
    - **Shared Memory (SHM)**: Large data payloads (like PyArrow tables) are passed via shared memory to avoid the overhead of serialization/deserialization between local processes.
    - **Routing**: Messages contain `route_keys` that determine their destination (e.g., `DAQJobStoreROOT`, `DAQJobRemote`).

4.  **Remote Communication (`DAQJobRemote` & `RemoteProxy`)**:
    - Utilizes **ZeroMQ (ZMQ)** for inter-supervisor communication.
    - Uses a PUB/SUB pattern for data distribution.
    - A `RemoteProxy` (XSUB/XPUB) acts as a central hub for multiple supervisors.

### Data Storage

- **ROOT Store**: Buffers data into PyArrow tables and writes them to `.root` files, a standard format in High Energy Physics (HEP).
- **Memory Store**: Accumulates data in memory for real-time access or benchmark "voiding".
- **CSV Store**: Simple tabular data storage for smaller datasets.

## Performance & Benchmarking

The system includes a specialized `benchmark.py` script to stress-test the entire pipeline from data generation to storage.

### Benchmark Capabilities

- Simulates multiple clients.
- Configurable payload sizes (number of rows/values per message).
- Real-time throughput (MB/s) and message rate metrics.
- Queue size monitoring to identify bottlenecks.

### Recent Performance Optimizations

- **Non-blocking I/O**: Receive threads in `DAQJobRemote` use non-blocking queue operations to prevent backpressure from stalling network intake.
- **ZMQ Tuning**: High Water Marks (HWM) are configured to handle bursts without dropping data or blocking.
- **Direct Routing**: Optimized the supervisor to push route updates to jobs, allowing them to send data directly to store queues.
- **Stats Granularity**: Increased frequency of stats reporting (0.1s interval) for accurate performance profiling.

## Tech Stack

- **Language**: Python 3.12+ (leveraging modern features like `msgspec` and `pyarrow`).
- **Networking**: ZeroMQ.
- **Data Format**: PyArrow (columnar memory format).
- **Serialization**: Pickle/Msgspec/SharedMemory.
- **Configuration**: TOML-based configuration files.
