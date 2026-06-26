# Welcome to ENRGDAQ

**ENRGDAQ** is the data acquisition (DAQ) framework used by the ENRG collaboration for
neutrino physics experiments. It handles the full pipeline — reading out hardware
sensors, processing data at multi-Gbps rates, and storing results for analysis.

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
