# Deployment

ENRGDAQ supports multi-machine deployments through **federation**.
Multiple supervisor nodes communicate in a star topology, with
one central server and any number of clients.

---

## Single machine (default)

Run the supervisor with your configs:

```bash
uv run python src/run.py --daq-job-config-path configs/my_run/
```

All DAQJobs run as separate processes on the same machine. The message
broker uses IPC ZMQ sockets (`ipc://` on Unix) for low-latency communication.
On Windows, `tcp://127.0.0.1` with dynamic ports is used as a fallback.

---

## Star topology federation

For multi-machine setups, one supervisor acts as the **server** (hub) and all others connect as **clients**.

---

## Server configuration

The server exposes two ZMQ endpoints that clients connect to:

```toml
# server_supervisor.toml
[info]
supervisor_id = "daq-server"
supervisor_tags = ["production", "hub"]

[federation]
is_server = true
server_xpub_url = "tcp://0.0.0.0:5560"   # Bind to all interfaces
server_xsub_url = "tcp://0.0.0.0:5561"   # Bind to all interfaces
```

The server runs its own DAQJobs in addition to forwarding messages:

```bash
uv run python src/run.py --daq-job-config-path configs/server/
```

The supervisor config (`supervisor.toml`) is loaded from inside the config
directory — there is no separate `--supervisor-config` flag.

---

## Client configuration

Each client connects to the server's endpoints:

```toml
# client_supervisor.toml
[info]
supervisor_id = "daq-client-1"
supervisor_tags = ["lab-bench-1"]

[federation]
remote_server_xpub_url = "tcp://192.168.1.10:5560"  # Server's IP
remote_server_xsub_url = "tcp://192.168.1.10:5561"  # Server's IP
```

The client runs its own DAQJobs locally:

```bash
uv run python src/run.py --daq-job-config-path configs/client/
```

---

## How messages flow in federation

Federation uses one-directional forwarding to prevent message loops:

1. **Client to Server**: Messages published locally on the client are
   forwarded to the server's XSUB endpoint.
2. **Server to All clients**: Messages published on the server (or forwarded
   from other clients) are distributed to all connected clients via XPUB.

This means every client receives messages from every other client.

---

## Message routing in federation

Most messages automatically target the **local** supervisor's stores by
default. To send data to a remote supervisor, the producer must set
`target_local_supervisor = false` in the store message.

This is controlled per-message, not per-config. If you want all messages
from a job to be globally visible, set `target_local_supervisor` in the
store config:

```toml
[store_config.csv]
file_path = "data.csv"
# Not directly settable in TOML — controlled by the job code
```

Remote message routing in federation is handled by the
`SupervisorFederationConfig` in the supervisor config. The server exposes
`server_xpub_url` / `server_xsub_url` endpoints, and clients connect via
`remote_server_xpub_url` / `remote_server_xsub_url`.

---

## CNC in federation

The Command & Control (CNC) system also operates in a star topology.
Configure CNC on both server and clients:

**Server:**

```toml
[cnc]
is_server = true
rest_api_enabled = true
rest_api_host = "0.0.0.0"
rest_api_port = 8000
```

Note: the default `rest_api_host` is `"localhost"`. Set to `"0.0.0.0"`
explicitly if you need remote access.

**Client:**

```toml
[cnc]
is_server = false
server_host = "192.168.1.10"   # Server's IP
server_port = 1638
```

The server's REST API at port 8000 provides a unified view of all
connected supervisors — jobs, stats, and logs.

---

## Standard Ports used by ENRGDAQ

For federation to work, these ports must be open:

| Port | Purpose | Direction |
|------|---------|-----------|
| 5560 | ZMQ XPUB (server) | Client → Server |
| 5561 | ZMQ XSUB (server) | Client → Server |
| 1638  | CNC ZMQ (server) | Client → Server |
| 8000  | CNC REST API (server) | Any → Server |

Adjust the ports in your config if they conflict with other services.

---

## Testing federation locally

You can test federation on a single machine by running two supervisors
on different config directories with different federation ports:

```bash
# Terminal 1: Server
uv run python src/run.py --daq-job-config-path configs/server/

# Terminal 2: Client
uv run python src/run.py --daq-job-config-path configs/client/
```

The server and client communicate over `localhost`. No network setup needed.

---

## Next steps

- [Monitoring](../operations/monitoring.md) — monitor a federated deployment
- [Troubleshooting](../operations/troubleshooting.md) — debug connection issues
