# Welcome to ENRGDAQ

**ENRGDAQ** is the data acquisition (DAQ) framework used by the ENRG collaboration for
neutrino physics experiments. It handles the full pipeline — reading out hardware
sensors, processing data at multi-Gbps rates, and storing results for analysis.

---

## Who is this for?

This documentation is written for **new lab members** joining the ENRG collaboration.
If you need to understand how data flows from detectors to disk, configure experiment
runs, or write a custom sensor integration, you are in the right place.

Already familiar with the system? The [API Reference](/api/) documents every module
and class in detail.

---

## Quick navigation

| I want to…                                    | Start here                                |
| --------------------------------------------- | ----------------------------------------- |
| Install ENRGDAQ on my machine                 | [Installation](getting-started/installation.md)  |
| Run my first DAQ job in 5 minutes             | [Quickstart](getting-started/quickstart.md)       |
| Understand how the system works               | [Architecture](reference/architecture.md)          |
| Set up hardware (CAEN digitizers, cameras…)   | [Hardware Setup](guides/hardware-setup.md)          |
| Write a custom sensor or storage backend      | [Creating a DAQJob](guides/creating-daqjob.md)      |
| Deploy to multiple machines                   | [Deployment](guides/deployment.md)                 |
| Monitor a running deployment                  | [Monitoring](operations/monitoring.md)             |

---

## What ENRGDAQ does

- **Hardware integration** — reads CAEN digitizers, high-voltage supplies,
  cameras, environmental sensors, and more.
- **High-throughput messaging** — ZMQ pub/sub broker routes data between
  sensor jobs and storage jobs with zero-copy shared memory for Gbps rates.
- **Pluggable storage** — write to CSV, HDF5, ROOT, MySQL, Redis, or raw
  binary files. Mix and match per sensor.
- **Supervised processes** — the supervisor monitors every job; if one
  crashes it restarts automatically.
- **Distributed scaling** — federate multiple supervisor nodes across
  machines for larger deployments.

---

## Key concepts

If you are new to ENRGDAQ, these three concepts are worth knowing up front:

1. **DAQJob** — a single independent process. A sensor driver, a storage
   backend, or a monitoring handler. Each reads a TOML config file.
2. **Message Broker** — the internal pub/sub bus. Jobs publish messages on
   named topics; other jobs subscribe to topics they care about.
3. **Supervisor** — the top-level process that spawns all jobs, hosts the
   message broker, and reports system statistics.

---

## Paper & citation

ENRGDAQ is described in our NIM-A paper:

> *"ENRGDAQ: A modular data acquisition framework for neutrino physics experiments"*  
> Journal of Instrumentation (JINST)

If you use ENRGDAQ in your work, please cite the paper. You can find the
LaTeX source and preprint in the
[repository](https://github.com/ENRG-tr/enrgdaq/tree/main/paper).
