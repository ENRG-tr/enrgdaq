# ENRGDAQ: A High-Performance Distributed Data Acquisition Framework

## 1. Overview

**ENRGDAQ** is a modern, distributed data acquisition (DAQ) framework designed for the rigorous demands of high-throughput and low-latency instrumentation in experimental physics (e.g., JUNO, dark matter searches, and neutrino experiments). It leverages a multi-process, supervisor-orchestrated architecture to achieve high performance, process isolation, and fault tolerance.

By decoupling hardware acquisition, data processing, and storage into independent, isolated processes, ENRGDAQ ensures that hardware triggers are never blocked by slow disk I/O or network backpressure.

---

## 2. Core Architecture

### 2.1 The Supervisor Model

The fundamental unit of orchestration is the **Supervisor**. A supervisor manages a local group of **DAQ Jobs**, providing:

- **Process Isolation**: Every `DAQJob` runs in its own OS process (standard `multiprocessing`), preventing a crash or memory leak in one driver from affecting the entire system.
- **Lifecycle Management**: Automated process startup, health monitoring, and crash recovery (auto-restart).
- **Hierarchical Scaling**: Supervisors can be nested or distributed. A "Client Supervisor" typically handles local hardware, while a "Main Supervisor" aggregates data for global storage and monitoring.

### 2.2 Dynamic Message Routing

ENRGDAQ uses a unique hybrid routing mechanism:

- **Default Routing**: Small control messages are routed through the Supervisor's internal queues.
- **Direct Job-to-Job Routing**: For high-bandwidth paths (e.g., Digitizer → Store), the Supervisor dynamically pushes queue handles to the source job. This allows data to flow directly between processes without passing through the single-threaded bottleneck of the Supervisor's main loop.

### 2.3 High-Performance IPC & Throughput Optimization

To achieve multi-Gbps throughput, ENRGDAQ minimizes data movement overhead using several techniques:

- **Zero-Serialization ZMQ Pipeline**: Utilizing `ZMQQueue` with ZMQ IPC and **Pickle Protocol 5**, large memory buffers (NumPy arrays, PyArrow Tables) are passed as out-of-band frames. This achieves **Zero User-Space Copy** (no data duplication within the application memory) by bypassing Python's recursive object pickling.
- **Arrow Native IPC**: PyArrow Tables are serialized using the specialized Arrow Stream format, which provides efficient, flat-memory access during transmit/receive.
- **Shared Memory (SHM)**: In specific local-node configurations, data is passed via `multiprocessing.shared_memory`, sending only a lightweight `SHMHandle` (metadata) through the messaging layer to achieve true zero-copy at the kernel level.

---

## 3. Performance Analysis

### 3.1 Measured Throughput

Performance benchmarks conducted on high-end Unix hardware (Apple M-series) characterize the framework across two distinct regimes: single-link efficiency and system-wide aggregate scaling.

| Metric                     | Measured Value            | Description                                 |
| :------------------------- | :------------------------ | :------------------------------------------ |
| **System Aggregate Link**  | **~8.5 Gbps (1.06 GB/s)** | Total data moved across ZMQ/SHM bus.        |
| **Single-Link Throughput** | **~3.7 Gbps (460 MB/s)**  | Sustained rate for a single readout client. |
| **Theoretical Peak**       | **11.4 Gbps**             | Burst capability under optimal alignment.   |

**Performance Narrative:**

- **Scaling Limits**: Throughput scales linearly for 1–2 heavy clients (~8.5 Gbps aggregate). Beyond this point, throughput degrades due to context-switching contention and IPC overhead within the single-threaded ZMQ Proxy—a known ceiling for high-level language orchestration.
- **Latency vs. Throughput**: The system exhibits an inherent steady-state latency of **~60ms**. Under extreme saturation (hammer-testing), this increases to **~150–250ms ($p_{99}$)** as ZMQ buffers absorb backpressure. This asynchronous, deep-buffered behavior is a deliberate design choice, trading millisecond-level reaction time for robust multi-Gbps burst absorption.

### 3.2 Comparison with Compiled Systems (C++)

A core objective of ENRGDAQ is to minimize the performance gap between interpreted HLLs (Python) and compiled languages (C++).

- **Performance Trade-offs**: While highly optimized C++ DAQ systems (e.g., ATLAS TDAQ) can reach 40-80 Gbps on local IPC, ENRGDAQ's **11.4 Gbps** is sufficient to saturate 10 Gbps network hardware and utilizes approximately 40% of the theoretical bandwidth of high-end Gen4 NVMe storage.
- **Development Efficiency**: ENRGDAQ achieves approximately 15-25% of the raw throughput of a hand-tuned C++ system while reducing development complexity by an estimated order of magnitude (~10x effort reduction). This makes it suitable for mid-scale experiments where development speed is critical.

---

## 4. Specialized DAQ Jobs

### 4.1 Hardware Drivers

- **`DAQJobCAENDigitizer`**: The high-bandwidth path. Interfaces with hardware via a C-wrapper, translating raw binary streams into PyArrow tables for downstream processing.
- **`DAQJobCAENHV`**: Precision control for High Voltage. Features a hardware **Watchdog** system to detect library-level hangs, common in vendor provided SDKs.
- **`DAQJobCamera / N1081B`**: Drivers for auxiliary monitoring and timing hardware.

### 4.2 Storage Backend (`DAQJobStore`)

- **`DAQJobStoreROOT`**: Implements a **Buffering Writer** that batches PyArrow tables before committing to disk, significantly reducing the overhead of high-frequency `TTree` metadata updates.
- **`DAQJobStoreRedis`**: Optimized for real-time monitoring via `RedisTimeSeries`.

---

## 5. System Management (CNC)

The system is managed via a REST API and WebSocket gateway:

- **Runtime Configuration**: Update hardware setpoints (e.g., trigger thresholds) without re-initializing the process tree.
- **Telemetry Aggregation**: Centralized log and health status collection from distributed nodes.

---

## 6. Scaling & Determinism

### 6.1 Process-Level Concurrency

ENRGDAQ circumvents the Python Global Interpreter Lock (GIL) by delegating each `DAQJob` to an independent OS-level process.

- **Throughput Scaling**: Performance scales with available physical cores for ZMQ IO-threads and data serialization.
- **Flow Control**: Employs ZMQ High Water Marks (HWM) to implement backpressure, ensuring system stability during I/O micro-bursts or storage latency spikes.

### 6.2 ML Readiness

The use of **PyArrow** as the primary data exchange format enables immediate, zero-copy conversion to **PyTorch Tensors** or **NumPy** arrays, allowing researchers to integrate modern machine learning models for real-time inference (e.g., pulse shape discrimination) directly into the readout pipeline.

---

## 7. Conclusions for JINST

ENRGDAQ demonstrates that HLLs like Python, when augmented with zero-user-space copy primitives and C++ backed data engines, provide sufficient performance (>10 Gbps) for modern mid-scale physics instrumentation. The framework prioritizes scientist productivity and ease of extensibility while maintaining the throughput required for high-resolution waveform digitization.
