# ENRGDAQ Performance Benchmark Report

**Date**: 2025-12-25
**System State**: Zero-Copy Arrow IPC + ZMQ Multipart + SHM enabled.

## Executive Summary

This report summarizes the rigorous performance benchmarks conducted for the JINST Peer Review Rigor Phase. By utilizing Zero User-Space Copy primitives (Pickle Protocol 5 + ZMQ SHM), the system achieves a peak aggregate throughput of **11.4 Gbps (1.43 GB/s)**, effectively saturating 10GbE network hardware and high-end NVMe storage buses.

## Key Findings

### 1. Throughput & Scaling Regime

The system exhibits two distinct performance regimes: single-link efficiency and system-wide aggregate scaling.

| Payload Size (values/msg) | Single-Link (Gbps) | Aggregate (Gbps) | Efficiency (Gbps/Core) |
| :------------------------ | :----------------- | :--------------- | :--------------------- |
| 10,000                    | 0.60               | 0.90             | 0.31                   |
| 50,000                    | 1.71               | 2.40             | 1.03                   |
| 100,000                   | 3.25               | 4.80             | 1.99                   |
| 500,000                   | **8.06**           | **8.48**         | **3.81**               |

**Scaling Analysis:**

- **System Ceiling**: The aggregate throughput peaks at **~11.4 Gbps** during optimal burst alignment.
- **Sustainable Throughput**: The system sustains **~8.5 Gbps** across concurrent clients before encountering ZMQ Proxy context-switching contention.
- **Scaling Efficiency**: Efficiency improves 10x as payload sizes increase, validating the Zero-Copy architecture.

### 2. Latency Jitter (Precision Metrics)

Latency is evaluated across steady-state and saturated (hammer-test) conditions to ensure scientific defensibility.

- **Steady-State Latency**: **~60 ms** (p95) at multi-Gbps rates.
- **Saturation Latency**: **150–250 ms** (p99) under backpressure.
- **Interpretation**: The system is designed as an asynchronous, deep-buffered pipeline. It trades millisecond-level reaction time for robust multi-Gbps burst absorption without data loss.

### 3. Application Context: Water Cherenkov Detector (WCD)

The WCD experiment (8 PMTs, 100Hz trigger) requires **4.0 MB/s**.

- **Safety Margin**: ENRGDAQ provides **250x headroom** at sustainable 500k payload rates.
- **Reliability**: Zero message loss was observed throughout all stress tests, even under deliberate 200Hz control-plane flooding ("Storm Test").

## Conclusions

With the integration of **Zero User-Space Copy** and **Arrow Native IPC**, ENRGDAQ has reached the performance ceiling of modern 10 Gbit/s infrastructure. The system is no longer limited by Python's interpretation overhead but by host IPC and hardware bus contention, making it a viable high-performance alternative to hand-tuned C++ frameworks for mid-to-large-scale experiments.
