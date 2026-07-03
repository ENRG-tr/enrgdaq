# Architecture

This page explains the internals of ENRGDAQ: how the supervisor, message
broker, DAQJobs, and shared memory work together to deliver high-throughput
data acquisition.

---

## Layers

### 1. Supervisor

A single `Supervisor` process manages the entire system:

- Reads TOML config files from the config directory
- Spawns each DAQJob as an independent OS process using `fork()`
- Monitors liveness via heartbeats and watchdog timers
- Restarts crashed jobs with configurable backoff
- Collects per-job statistics (message counts, latency, CPU/RSS)
- Hosts the CNC command server for remote management

### 2. Message Broker

The broker runs inside the supervisor process. It uses ZMQ's built-in
`zmq.proxy()` between XPUB and XSUB sockets:

- **XSUB** — receives messages from producers
- **XPUB** — distributes messages to subscribers based on topic prefix matching

All messages are serialized with **pickle** (not msgspec). This is deliberate:
pickle supports out-of-band `PickleBuffer` for zero-copy shared memory
transfers, which msgspec does not.

### 3. Process Pool: DAQJobs

Each DAQJob runs as a separate process with three daemon threads:

| Thread | Role |
|--------|------|
| `_consume_thread` | Receives messages via ZMQ SUB, puts them on `message_in` queue |
| `_publish_thread` | Takes messages from `message_out` queue, publishes via ZMQ PUB |
| `_report_thread` | Periodically sends stats and trace reports to supervisor (1 Hz) |

Jobs use `_put_message_out()` to send data and `handle_message()` to receive.
Every job is **isolated** — a crash in one job does not affect others.

### 4. Data Plane: Shared Memory

Bulk data (waveforms, PyArrow tables) bypasses ZMQ entirely, if sent to own ENRGDAQ instance (not remote).
The producer writes data directly to a slot in a **pre-allocated shared memory ring
buffer**. The consumer reads from the same slot.

---

## Fault tolerance

The supervisor monitors all DAQJobs and recovers from failures:

1. **Watchdog** — each DAQJob has a watchdog timer. If the main thread
   hangs (e.g., stuck in a hardware read), the watchdog force-kills the
   process with `os._exit(1)`.
2. **Heartbeat** — DAQJobs send periodic heartbeat messages. If the
   supervisor doesn't receive heartbeats from a job, it assumes the
   process is dead.
3. **Restart** — the supervisor restarts crashed jobs
4. **Isolation** — jobs are independent OS processes. A segmentation
   fault in one job does not crash others.

---

## Multi-machine federation

For deployments spanning multiple machines, ENRGDAQ uses a star topology:

- One **server** supervisor exposes XPUB/XSUB endpoints
- **Client** supervisors connect to the server and forward their messages
- The server relays messages between all clients

Messages are forwarded in one direction. (client → server → all clients)

---

## Class hierarchy

All DAQJobs inherit from `DAQJob`. Storage backends inherit from
`DAQJobStore` (which itself inherits from `DAQJob`)

---

## Control plane (CNC)

The Command & Control system provides remote management:

- **ZMQ ROUTER/DEALER** — binary request/response protocol for CNC commands
  (restart jobs, check status, send messages)
- **FastAPI REST API** — HTTP wrapper around the ZMQ protocol, provides
  `/status`, `/clients`, `/jobs`, `/restart`, `/templates` endpoints
- **Start topology** — one CNC server, multiple CNC clients

## Next steps

- [Message Flow](message-flow.md) — detailed topic routing and zero-copy paths
- [Storage Backends](storage-backends.md) — comparison of all 7 store types
