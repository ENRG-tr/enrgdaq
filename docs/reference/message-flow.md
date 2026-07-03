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
_put_message_out calls _prepare_message()
    │
    ▼
_prepare_message: try_zero_copy_pyarrow()    # If PyArrow + SHM: write to ring buffer
    │                                           Replace table with RingBufferHandle
    ▼
_prepare_message: pickle SHM fallback        # If non-PyArrow + SHM: pickle to SharedMemory
    │
    ▼
_publish_buffer queue
    │
    ▼
_publish_thread             # ZMQ PUB socket, reads from _publish_buffer
    │
    ▼
Supervisor (zmq.proxy)      # XSUB → XPUB forwarding
    │
    ▼
Subscriber's ZMQ SUB        # Topic prefix match
    │
    ▼
_consume_thread             # ZMQ SUB receives, calls handle_message() directly
    │
    ▼
handle_message(message)     # Consumer processes
```

!!! note
    The `_consume_thread` calls `handle_message()` **directly** — there is no
    intermediate `message_in` queue in the consume path. The `message_in` attribute
    exists but is not used by the default consume thread.

---

## Topic routing

Every message carries a `topics` set (a set of strings). The `pre_send()`
method computes topics automatically based on the message type and config.

### Store message routing

When a producer sends a `DAQJobMessageStore*` with a `store_config`,
the system inspects which store types are configured in the message's
`pre_send()` and generates store topics based on the store config field names:

```python
# In DAQJobMessageStore.pre_send():
# For each non-None field on store_config (e.g., csv, root, hdf5),
# generates topics like store.DAQJobStoreCSV, store.DAQJobStoreROOT, etc.
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
- `supervisor.{id}.internal` — internal messages (job started, stop, routes)

!!! note "Topic format"
    The topic format is `supervisor.{id}.internal` (with the supervisor ID
    **before** `internal`), not `supervisor.internal.{id}`.

This scoping prevents cross-supervisor leakage in federated deployments.

---

## Two-tier zero-copy system

ENRGDAQ uses two zero-copy strategies depending on the message type.

### Tier 1: PyArrow ring buffer (fastest)

When a producer sends `DAQJobMessageStorePyArrow`:

1. **Claim a slot** in the shared memory ring buffer
2. **Write Arrow IPC** directly into the slot using
   `pa.ipc.new_stream(sink, schema)` + `writer.write_table(table)`
3. **Replace the table** in the message with a `RingBufferHandle`
   (only metadata: buffer name, slot index, data size)
4. **Send the handle** over ZMQ (few bytes)
5. The consumer calls `handle.load_pyarrow()` which uses
   `pa.foreign_buffer(address, size, base=ring_buffer)` for a
   true zero-copy read
6. The consumer calls `handle.release()` to free the slot

This path achieves **zero user-space copies** for the bulk data.
Only the metadata handle travels over ZMQ.
Shared memory is used when `use_shm_when_possible = True` on the
`DAQJobConfig` — there is no per-message `use_shm` flag.

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
| `DAQJobMessageHeartbeat` | Liveness signal | Inherits timestamp only, no additional fields |

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

Every message carries a unique `id` (UUID). Trace events are collected
automatically in each DAQJob's `_trace_events` list. The `_report_thread`
publishes trace reports periodically (every ~1 second) to the
`traces.supervisor.{id}` topics. This enables end-to-end latency
measurement across processes and machines.

Tracing is always active — there is no toggle to disable it.

---

## Next steps

- [Architecture](architecture.md) — system overview
- [Storage Backends](storage-backends.md) — comparison of all store types
