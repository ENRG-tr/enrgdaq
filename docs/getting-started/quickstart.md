# Quickstart

This guide gets you from zero to a running DAQ system in 5 minutes using the
built-in `DAQJobTest` job. No hardware required.

---

## 1. Create a config file

Create a TOML file for the test job:

```bash
mkdir -p configs/my_first_run
```

Save the following as `configs/my_first_run/test_job.toml`:

```toml
daq_job_type = "DAQJobTest"
rand_min = 1
rand_max = 100
interval_seconds = 1.0

[store_config.csv]
file_path = "out/test_data.csv"
add_date = true
overwrite = false
```

This job generates random numbers once per second and writes them to a CSV file.

---

## 2. Start the supervisor

The **supervisor** is the top-level process that reads all config files, spawns
each job, and runs the message broker.

```bash
uv run python src/run.py --daq-job-config-path configs/my_first_run
```

You should see output like:

```
[INFO] Supervisor initializing...
[INFO] Starting message broker...
[INFO] Spawning DAQJobTest (jid=1)...
[INFO] DAQJobTest (jid=1) started
```

The supervisor runs until you stop it with `Ctrl+C`.

---

## 3. Check the output

The test job writes random numbers to `out/test_data.csv`. After a few seconds:

```bash
cat out/test_data.csv
```

You will see rows with timestamp, random value, and job metadata.

Two additional statistics files are generated automatically:

| File | Contents |
|------|----------|
| `out/stats.csv` | Per-job message counts and latencies |
| `out/stats_remote.csv` | Aggregated throughput in MB/s |

---

## 4. Stop the system

Press `Ctrl+C` in the terminal running the supervisor. All DAQJobs
are stopped gracefully.

---

## What just happened?

```
                    ┌─────────────────┐
configs/            │   Supervisor     │
  my_first_run/     │                  │
    test_job.toml ─►│  ┌─────────────┐ │
                    │  │ Msg Broker  │ │
                    │  │ (XPUB/XSUB) │ │
                    │  └──────┬──────┘ │
                    │         │        │
                    │  ┌──────▼──────┐ │
                    │  │  DAQJobTest │ │
                    │  │  (producer) │ │
                    │  └──────┬──────┘ │
                    │         │        │
                    │  ┌──────▼──────┐ │
                    │  │DAQJobStoreCSV│ │
                    │  │  (consumer) │ │
                    │  └─────────────┘ │
                    └─────────────────┘
```

1. The supervisor read `test_job.toml`, created a `DAQJobTest` config,
   and spawned the job process.
2. `DAQJobTest` generates random numbers and publishes them as messages.
3. The `store_config.csv` field tells the system to route those messages
   to `DAQJobStoreCSV`.
4. `DAQJobStoreCSV` receives the messages and writes them to disk.

No code changes needed — everything is driven by the TOML config.

---

## Next steps

- [Configuration](configuration.md) — learn all config options
- [Creating a DAQJob](../guides/creating-daqjob.md) — write your own sensor job
- [Hardware Setup](../guides/hardware-setup.md) — connect real sensors
