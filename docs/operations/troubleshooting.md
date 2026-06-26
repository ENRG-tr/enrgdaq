# Troubleshooting

Common issues and how to diagnose them.

---

## A DAQJob keeps crashing and restarting

**Symptoms:** The supervisor log shows repeated restart messages for
the same job.

**Diagnosis:**

1. Check the supervisor log for the crash reason:
   ```bash
   # Look for ERROR lines mentioning the job name
   grep "ERROR.*YourJobName" out/supervisor.log
   ```

2. Run the job in debug mode to see the actual exception:
   ```toml
   verbosity = "DEBUG"
   ```

3. Check if the hardware is connected and responding. For CAEN jobs,
   verify:
   - Optical link fibers are properly seated
   - Device is powered on
   - IP address is reachable (for HV supplies)

**Common causes:**

| Cause | Fix |
|-------|-----|
| Hardware not reachable | Check cables, power, network |
| Permission denied (USB/PCIe) | Run with `sudo` or add udev rules |
| Config field typo | Compare against example configs in `configs/examples/` |
| Missing Python dependency | `uv sync` to reinstall |

---

## No data in output files

**Symptoms:** The output CSV/ROOT/etc. file exists but is empty.

**Diagnosis:**

1. Verify the store job is running:
   ```bash
   grep "DAQJobStore" out/stats.csv
   ```
   The `is_alive` column should be `True`.

2. Check that the store config is set on the producer:
   ```toml
   [store_config.csv]
   file_path = "data.csv"
   ```
   Without a `store_config` section, no store topics are generated.

3. Enable debug logging to see if messages are being published:
   ```toml
   verbosity = "DEBUG"
   ```

4. Check if messages are routing to the correct topic:
   - The store job subscribes to `store.<StoreClassName>`
   - The producer must include matching store config types

---

## High latency (> 100ms p95)

**Symptoms:** `stats.csv` shows `latency_p95_ms` over 100ms.

**Diagnosis:**

1. Check if shared memory is working:
   ```toml
   # In the job config
   use_shm_when_possible = true
   ```
   If the ring buffer can't be created (e.g., insufficient memory),
   the system falls back to ZMQ without warning.

2. Verify ring buffer sizing:
   ```toml
   # In supervisor config
   ring_buffer_size_mb = 512
   ring_buffer_slot_size_kb = 1024   # Should be larger than largest message
   ```

3. Check for consumer backpressure:
   - If the ring buffer is full, producers block
   - Increase `ring_buffer_size_mb` or decrease message rate

4. Check CPU usage:
   ```bash
   grep "cpu_percent" out/stats.csv
   ```
   If any job is near 100%, it may be a bottleneck.

---

## ZMQ connection errors

**Symptoms:** Log shows `ZMQError: Connection refused` or
`ZMQError: Address already in use`.

**Diagnosis:**

1. Port conflicts (federation or CNC):
   ```bash
   # Check if the port is already in use
   lsof -i :16382   # macOS/Linux
   netstat -ano | findstr :16382  # Windows
   ```
   Change the port in the config if needed.

2. Firewall blocking federation ports. Ensure ports 16382, 16383,
   1638 are open between machines.

3. Multiple supervisors on the same machine. Only one supervisor
   can bind to the federation ports at a time.

---

## Shared memory errors

**Symptoms:** `PermissionError`, `FileNotFoundError`, or
`BusError` related to `/dev/shm`.

**Diagnosis:**

1. Check `/dev/shm` space:
   ```bash
   df -h /dev/shm
   ```
   If it's full, clean up old shared memory segments:
   ```bash
   # List shared memory segments
   ls -la /dev/shm/
   ```
   Unused segments can be removed manually or by restarting the machine.

2. On macOS, shared memory is handled by the kernel automatically.
   If you see errors, try setting `use_shm_when_possible = false`.

3. Ring buffer size exceeds available memory:
   ```toml
   ring_buffer_size_mb = 256  # Reduce if needed
   ```

---

## CNC REST API not responding

**Symptoms:** `curl http://localhost:8000/status` hangs or returns
connection refused.

**Diagnosis:**

1. Verify CNC is enabled in the supervisor config:
   ```toml
   [cnc]
   is_server = true
   rest_api_enabled = true
   rest_api_host = "0.0.0.0"
   rest_api_port = 8000
   ```

2. Check if the port is already in use (see ZMQ section above).

3. Try binding to `localhost` instead of `0.0.0.0` if only local
   access is needed.

4. Check supervisor log for CNC initialization errors.

---

## Camera not detected (macOS)

**Symptoms:** `DAQJobCamera` fails with `camera_device_index not found`.

**Diagnosis:**

1. macOS security requires camera permission. Grant terminal/IDE
   access in System Preferences → Security & Privacy → Camera.

2. Use `camera_device_name` instead of index:
   ```toml
   camera_device_name = "FaceTime HD Camera"
   ```

3. The camera job uses `multiprocessing_method = "spawn"` instead of
   `fork` on macOS to avoid AVCaptureDevice issues.

---

## Windows-specific issues

- Shared memory ring buffers are **not supported**. Set
  `use_shm_when_possible = false` globally.
- `fork()` is not available. DAQJobs run as threads instead of
  processes.
- Some hardware SDKs (CAEN, N1081B) are Linux-only.

---

## Getting help

If you can't resolve an issue:

1. Enable debug logging and capture the full output:
   ```bash
   uv run python src/run.py --daq-job-config-path configs/ 2>&1 | tee debug.log
   ```

2. Check the GitHub repository for known issues:
   [github.com/ENRG-tr/enrgdaq/issues](https://github.com/ENRG-tr/enrgdaq/issues)

3. Provide the debug log, your config files (remove sensitive data),
   and platform details (OS, Python version) when reporting.
