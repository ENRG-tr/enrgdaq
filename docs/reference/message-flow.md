# Message Flow

This page explains how messages travel from a producer to a consumer,
including topic-based routing, the two-tier zero-copy system, and
fallback paths.

---

## Overview

Messages in ENRGDAQ follow this path:

```
Producer._put_message_out(message)
    │
    ▼
message.pre_send()          # Auto-compute topics from store_config
    │
    ▼
try_zero_copy_pyarrow()     # If PyArrow + SHM enabled: write to ring buffer
    │                         Replace table with RingBufferHandle
    ▼
message_out queue
    │
    ▼
_publish_thread             # ZMQ PUB socket
    │
    ▼
Supervisor (zmq.proxy)      # XSUB → XPUB forwarding
    │
    ▼
Subscriber's ZMQ SUB        # Topic prefix match
    │
    ▼
_consume_thread             # ZMQ SUB receives
    │
    ▼
message_in queue
    │
    ▼
handle_message(message)     # Consumer processes
```

---

## Topic routing

Every message carries a `topics` set (a set of strings). The `pre_send()`
method computes topics automatically based on the message type and config.

### Store message routing

When a producer sends a `DAQJobMessageStore*` with a `store_config`,
the system inspects which store types are configured:

```python
# Simplified: what happens in pre_send()
store_config = message.store_config  # e.g., {csv: ..., root: ...}
for store_type in store_config.store_types:
    for store_job in store_type_to_job[store_type]:
        message.topics.add(f"store.{store_job.__name__}")
```

So a message with `store_config = {csv: {...}, hdf5: {...}}` gets topics:

- `store.DAQJobStoreCSV`
- `store.DAQJobStoreHDF5`

The store jobs subscribe to `store.<ClassName>` automatically. The ZMQ
proxy matches topic prefixes:

```
Publisher: message with topic "store.DAQJobStoreCSV"
    │
    ▼ zmq.proxy() prefix matching
    │
Subscriber: subscribed to "store.DAQJobStoreCSV"  ✓ match!
Subscriber: subscribed to "store.DAQJobStoreROOT"  ✗ no match
```

### Internal message routing

Messages between a DAQJob and its supervisor use supervisor-scoped topics:

- `stats.supervisor.{id}` — stats reports from jobs to supervisor
- `traces.supervisor.{id}` — trace reports from jobs to supervisor
- `supervisor.internal.{id}` — internal messages (job started, stop, routes)

This scoping prevents cross-supervisor leakage in federated deployments.

---

## Two-tier zero-copy system

ENRGDAQ uses two zero-copy strategies depending on the message type.

### Tier 1: PyArrow ring buffer (fastest)

When a producer sends `DAQJobMessageStorePyArrow` with `use_shm=True`:

1. **Claim a slot** in the shared memory ring buffer
2. **Write Arrow IPC** directly into the slot (`pa.ipc.write_table()`)
3. **Replace the table** in the message with a `RingBufferHandle`
   (only metadata: buffer name, slot index, data size)
4. **Send the handle** over ZMQ (few bytes)
5. The consumer calls `handle.load_pyarrow()` which uses
   `pa.foreign_buffer(address, size, base=ring_buffer)` for a
   true zero-copy read
6. The consumer calls `handle.release()` to free the slot

This path achieves **zero user-space copies** for the bulk data.
Only the metadata handle travels over ZMQ.

### Tier 2: pickle-in-SharedMemory (one copy saved)

For non-PyArrow messages with `use_shm_when_possible=True`:

1. The full message is pickled
2. The pickle bytes are written to a `multiprocessing.SharedMemory` block
3. A `SHMHandle` (name + size) is sent over ZMQ
4. The consumer reads from shared memory and unpickles

This saves one copy compared to sending the full payload over ZMQ,
but still involves pickling and unpickling.

### Fallback: normal ZMQ (always available)

If shared memory is unavailable (e.g., on Windows, or if
`use_shm_when_possible=False`), the full pickled message travels
over ZMQ directly. This is the simplest and most compatible path,
but has the highest overhead.

---

## Message types

ENRGDAQ defines a hierarchy of message types, all inheriting from
`DAQJobMessage` (a `msgspec.Struct`):

| Message type | Purpose | Data format |
|-------------|---------|-------------|
| `DAQJobMessageStoreRaw` | Binary blobs | `data: bytes` |
| `DAQJobMessageStoreTabular` | Row-and-column data | `keys: list[str]`, `data: list[list]` |
| `DAQJobMessageStorePyArrow` | Columnar numerical data | `table: pa.Table` or `handle: RingBufferHandle` |
| `DAQJobMessageStatsReport` | Periodic stats | Counts, latency, resource usage |
| `DAQJobMessageTraceReport` | Message trace events | List of per-message timing events |
| `DAQJobMessageJobStarted` | Job lifecycle | Signals process started |
| `DAQJobMessageStop` | Shutdown signal | Reason string |
| `DAQJobMessageHeartbeat` | Liveness check | Heartbeat timestamp |

---

## Serialization: why pickle?

Despite all models being `msgspec.Struct`, the wire format is **pickle**.
This is because pickle protocol 5 supports **out-of-band buffers**
(`PickleBuffer`) — essential for zero-copy shared memory transfers.

`msgspec` does not support out-of-band buffers, so ENRGDAQ uses
`pickle.dumps()` / `pickle.loads()` in `message_broker.py:send_message()`.

!!! warning "Security note"
    Pickle deserialization can execute arbitrary code. **Do not expose**
    the ZMQ endpoints to untrusted networks. Federation is designed for
    use within a trusted lab network.

---

## Per-message tracing

Every message carries a unique `id` (UUID). When `handle_traces` is
enabled, the system tracks:

- When a message was sent (producer timestamp)
- When a message was received (consumer timestamp)
- Message type, size, source job, and source supervisor

This enables end-to-end latency measurement across processes and machines.
Trace events are published to `traces.supervisor.{id}` topics.

---

## Next steps

- [Architecture](architecture.md) — system overview
- [Storage Backends](storage-backends.md) — comparison of all store types
