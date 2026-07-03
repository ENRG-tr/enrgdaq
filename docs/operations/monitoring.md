# Monitoring

This page covers how to monitor a running ENRGDAQ deployment — stats files,
the CNC REST API, healthchecks, and logging.

---

## Statistics files

ENRGDAQ generates two statistics files automatically. They are written to
the output directory (default: `out/`).

### stats.csv: per-job metrics

One row per DAQJob, updated in real time:

| Column | Description |
|--------|-------------|
| `supervisor` | Supervisor node ID |
| `daq_job` | DAQJob type name |
| `is_alive` | Whether the process is alive (true/false) |
| `last_message_in_date` | Timestamp of last message received |
| `message_in_count` | Total messages received |
| `last_message_out_date` | Timestamp of last message sent |
| `message_out_count` | Total messages sent |
| `message_in_queue_size` | Current message input queue depth |
| `message_out_queue_size` | Current message output queue depth |
| `last_restart_date` | Timestamp of last restart |
| `restart_count` | Total number of restarts |

Latency (avg, p95, p99), CPU percentage, and RSS memory are stored
separately as timeseries data via the stats handler's `timeseries_store_config`.
They do **not** appear as columns in `stats.csv`.

Example:

```csv
supervisor,daq_job,is_alive,last_message_in_date,message_in_count,restart_count
lab-server-1,DAQJobCAENDigitizer,true,1749000000000,15234,0
lab-server-1,DAQJobStoreROOT,true,1749000000000,15234,0
```

### stats_remote.csv: throughput summary

Aggregated supervisor-level metrics:

| Column | Description |
|--------|-------------|
| `supervisor` | Supervisor node name |
| `is_alive` | Whether the supervisor is active |
| `last_active` | Timestamp of last activity |
| `message_in_count` | Total messages received |
| `message_in_megabytes` | Total data received (MB) |
| `message_out_count` | Total messages sent |
| `message_out_megabytes` | Total data sent (MB) |

---

## CNC REST API

When CNC is enabled (`cnc.rest_api_enabled = true`), ENRGDAQ exposes a
REST API for remote monitoring and control.

### Endpoints

All endpoints are namespaced under `/clients/{client_id}/` where `client_id` is the
supervisor ID of the target node.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/clients` | List of connected CNC clients |
| `GET` | `/clients/{id}/status` | Full supervisor and job status |
| `GET` | `/clients/{id}/logs` | Recent log entries from the supervisor |
| `GET` | `/templates/daqjobs` | JSON schemas for all job configs |
| `GET` | `/templates/messages` | JSON schemas for all message types |
| `GET` | `/templates/stores` | JSON schemas for all store configs |
| `POST` | `/clients/{id}/ping` | Ping a connected client |
| `POST` | `/clients/{id}/restart_daq` | Restart the DAQ system |
| `POST` | `/clients/{id}/stop_daqjobs` | Stop all DAQJobs |
| `POST` | `/clients/{id}/stop_daqjob` | Stop a specific DAQJob |
| `POST` | `/clients/{id}/run_custom_daqjob` | Run a custom DAQJob ad-hoc |
| `POST` | `/clients/{id}/send_message` | Inject a message into the broker |

### Example: check system status

```bash
curl http://localhost:8000/clients | python -m json.tool
# Then use the client ID from the response:
curl http://localhost:8000/clients/<supervisor_id>/status | python -m json.tool
```

### Example: stop a specific job

The `stop_daqjob` endpoint accepts a JSON body with the following fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `daq_job_unique_id` | string | No | Unique ID of the job to stop |
| `daq_job_name` | string | No | Name (type) of the job to stop |
| `remove` | bool | No (default: `false`) | Whether to remove the job config after stopping |

At least one of `daq_job_unique_id` or `daq_job_name` should be provided.

```bash
curl -X POST http://localhost:8000/clients/<supervisor_id>/stop_daqjob \
  -H "Content-Type: application/json" \
  -d '{"daq_job_unique_id": "<job_unique_id>", "remove": false}'
```

---

## Healthcheck

The `DAQJobHealthcheck` job monitors combined DAQJob statistics (from
`DAQJobMessageCombinedStats`) and sends alerts when configured
conditions are violated.

### Configuration

```toml
daq_job_type = "DAQJobHealthcheck"

[[healthcheck_stats]]
daq_job_type = "DAQJobCAENDigitizer"
stats_key = "message_out_stats"
alert_if_interval_is = "unsatisfied"   # Alert if NOT updated recently
interval = "30s"                       # Must be updated within 30 seconds
alert_info.message = "Digitizer stopped sending data!"
alert_info.severity = "error"

[[healthcheck_stats]]
daq_job_type = "DAQJobStoreROOT"
stats_key = "message_in_stats"
alert_if_interval_is = "unsatisfied"
interval = "30s"
alert_info.message = "ROOT store not receiving data!"
alert_info.severity = "warning"
```

!!! warning "Severity values are lowercase"
    Valid severity values are `"info"`, `"warning"`, and `"error"` (lowercase).
    Using uppercase like `"ERROR"` will fail to serialize.

### Alert conditions

| Condition | Meaning |
|-----------|---------|
| `satisfied` | Alert if the condition IS met (e.g., restart happened) |
| `unsatisfied` | Alert if the condition is NOT met (e.g., no recent data) |

### Interval format

Intervals are expressed as a number + unit:

| Format | Duration |
|--------|----------|
| `30s` | 30 seconds |
| `5m` | 5 minutes |
| `1h` | 1 hour |

The default `enable_alerts_on_restart = true` automatically alerts when
any DAQJob crashes and is restarted within the last minute.

### Alert delivery

Alerts are routed through the `DAQJobAlert` system. The built-in
`DAQJobAlertSlack` sends alerts to a Slack webhook. Custom alert handlers
can be created by subclassing `DAQJobAlert`.

---

## Logging

ENRGDAQ uses Python's `logging` module with `coloredlogs` for
terminal output.

### Log verbosity

Set per-job or globally via the `verbosity` field:

```toml
verbosity = "DEBUG"  # DEBUG, INFO, WARNING, ERROR
```

- `DEBUG` — detailed per-message logging (very verbose, for development)
- `INFO` — job lifecycle events (start, stop, restarts)
- `WARNING` — non-critical issues
- `ERROR` — failures that may affect data acquisition

### Supervisor-level logging

The supervisor log shows system-wide events. The exact format includes
timestamps, hostname, logger name, and log level:

```
2026-01-01 12:00:00 hostname Supervisor(lab-server-1) INFO Supervisor initializing...
2026-01-01 12:00:01 hostname Supervisor(lab-server-1) INFO Starting DAQJobCAENDigitizer
2026-01-01 12:00:05 hostname Supervisor(lab-server-1) ERROR DAQJobCAENDigitizer crashed
```

### Log forwarding (CNC)

When CNC is enabled, clients can request remote logs:

```bash
curl http://localhost:8000/clients/<supervisor_id>/logs | python -m json.tool
```

---

## Next steps

- [Troubleshooting](troubleshooting.md) — diagnose common issues
- [Deployment](../guides/deployment.md) — multi-machine deployments
