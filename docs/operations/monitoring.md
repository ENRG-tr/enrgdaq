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
| `job_name` | DAQJob type name |
| `unique_id` | Unique job instance identifier |
| `supervisor_id` | Supervisor node name |
| `message_in_count` | Total messages received |
| `message_out_count` | Total messages sent |
| `latency_avg_ms` | Average end-to-end message latency |
| `latency_p95_ms` | 95th percentile latency |
| `latency_p99_ms` | 99th percentile latency |
| `cpu_percent` | Process CPU usage (%) |
| `rss_mb` | Process RSS memory (MB) |
| `is_alive` | Whether the process is alive (True/False) |

Example:

```csv
job_name,unique_id,message_in_count,message_out_count,latency_avg_ms,cpu_percent,rss_mb,is_alive
DAQJobCAENDigitizer,jid_1,0,15234,1.92,12.3,45.2,True
DAQJobStoreROOT,jid_2,15234,0,0.00,8.1,120.7,True
```

### stats_remote.csv: throughput summary

Aggregated supervisor-level metrics:

| Column | Description |
|--------|-------------|
| `supervisor_id` | Supervisor node name |
| `mb_per_second` | Aggregate throughput (MB/s) |

---

## CNC REST API

When CNC is enabled (`cnc.rest_api_enabled = true`), ENRGDAQ exposes a
REST API for remote monitoring and control.

### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/status` | All supervisor and job status |
| `GET` | `/clients` | List of connected CNC clients |
| `GET` | `/jobs` | List of all running DAQJobs |
| `GET` | `/clients` | List of connected supervisors |
| `GET` | `/templates/configs` | JSON schemas for all job configs |
| `GET` | `/templates/messages` | JSON schemas for all message types |
| `GET` | `/templates/stores` | JSON schemas for all store configs |
| `POST` | `/restart` | Restart the DAQ system |
| `POST` | `/restart-daqjobs` | Restart specific DAQJobs |
| `POST` | `/stop-daqjob` | Stop a specific DAQJob |
| `POST` | `/run-daqjob` | Run a custom DAQJob ad-hoc |
| `POST` | `/send-message` | Inject a message into the broker |

### Example: check system status

```bash
curl http://localhost:8000/status | python -m json.tool
```

### Example: restart a specific job

```bash
curl -X POST http://localhost:8000/restart-daqjobs \
  -H "Content-Type: application/json" \
  -d '{"daq_job_unique_ids": ["jid_1"]}'
```

---

## Healthcheck

The `DAQJobHealthcheck` job monitors DAQJob statistics and sends alerts
when configured conditions are violated.

### Configuration

```toml
daq_job_type = "DAQJobHealthcheck"

[[healthcheck_stats]]
daq_job_type = "DAQJobCAENDigitizer"
stats_key = "message_out_stats"
alert_if_interval_is = "unsatisfied"   # Alert if NOT updated recently
interval = "30s"                       # Must be updated within 30 seconds
alert_info.message = "Digitizer stopped sending data!"
alert_info.severity = "ERROR"

[[healthcheck_stats]]
daq_job_type = "DAQJobStoreROOT"
stats_key = "message_in_stats"
alert_if_interval_is = "unsatisfied"
interval = "30s"
alert_info.message = "ROOT store not receiving data!"
alert_info.severity = "WARNING"
```

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

The supervisor log shows system-wide events:

```
[INFO] Supervisor initializing...
[INFO] Starting message broker on inproc://...
[INFO] Spawning DAQJobCAENDigitizer (jid=1)...
[INFO] Spawning DAQJobStoreROOT (jid=2)...
[ERROR] DAQJobCAENDigitizer (jid=1) crashed! Restarting in 1s...
```

### Log forwarding (CNC)

When CNC is enabled, clients can request remote logs:

```bash
curl -X POST http://localhost:8000/send-message \
  -H "Content-Type: application/json" \
  -d '{"type": "req_log", "job_id": "jid_1"}'
```

---

## Next steps

- [Troubleshooting](troubleshooting.md) — diagnose common issues
- [Deployment](../guides/deployment.md) — multi-machine deployments
