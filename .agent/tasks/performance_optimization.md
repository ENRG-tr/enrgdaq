---
description: Optimize DAQ throughput to multi-Gbps
---

The goal is to increase the DAQ throughput from ~125 MB/s to multi-Gbps speeds by eliminating serialization bottlenecks and leveraging Arrow IPC with Shared Memory.

### Phase 1: Diagnostics and Benchmarking

- [ ] Profile the serialization overhead of `pa.Table` using Pickle vs Arrow IPC.
- [ ] Measure the overhead of `np.random.random` in the benchmark job.

### Phase 2: Specialized Serialization

- [ ] Implement `__getstate__` and `__setstate__` or a specialized handler for `DAQJobMessageStorePyArrow` to use Arrow IPC instead of Pickle.
- [ ] Update `ZMQQueue` to optionally support "out-of-band" large data via Shared Memory.

### Phase 3: Zero-Copy Optimization

- [ ] Refactor `DAQJobBenchmark` to reuse PyArrow buffers if possible.
- [ ] Integrate Arrow's native IPC over Shared Memory handles ($SHM).

### Phase 4: Validation

- [ ] Run the benchmark and verify multi-Gbps throughput.
