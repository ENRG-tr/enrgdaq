# Hardware Setup

ENRGDAQ integrates with several hardware devices commonly used in
neutrino physics experiments. Each device type has a corresponding
DAQJob that handles initialization, data readout, and error recovery.

---

## CAEN Digitizer (Waveform Acquisition)

The `DAQJobCAENDigitizer` reads waveform data from CAEN digitizer cards.

### Requirements

- CAEN digitizer hardware (e.g., DT5742, V1742, DT5730)
- CAEN libraries installed (`caen-libs` Python package)
- Optical link or USB connection to the host machine

### Configuration

```toml
daq_job_type = "DAQJobCAENDigitizer"

# Connection
connection_type = "OPTICAL_LINK"   # USB, PCI_EXPRESS_A4818, OPTICAL_LINK
link_number = "1"
conet_node = 0
vme_base_address = 0

# Channel mask (bitmask, e.g., 0b00000011 for channels 0 and 1)
channel_enable_mask = 0b00000001

# Acquisition
record_length = 1024               # Samples per waveform
max_num_events_blt = 1

# Trigger
sw_trigger_mode = 1                # 0=DISABLED, 1=ACQ_ONLY, 2=EXTOUT_ONLY
channel_self_trigger_threshold_mv = 100
channel_self_trigger_channel_mask = 0b00000001

# Baseline position
baseline_position = "TOP"          # TOP, MIDDLE, BOTTOM

# Storage
[waveform_store_config.raw]
file_path = "waveforms.raw"
add_date = true

[stats_store_config.csv]
file_path = "digitizer_stats.csv"
add_date = true
```

### Connection types

| Type | Description |
|------|-------------|
| `USB` | Direct USB connection |
| `OPTICAL_LINK` | Fiber optic link via A3818/A4818 |
| `PCI_EXPRESS_A4818` | PCIe board model A4818 |
| `PCI_EXPRESS_A2818` | PCIe board model A2818 |

### Troubleshooting

- If the digitizer fails to initialize, check the optical link fiber connections
- If `record_length` is too large for the digitizer's memory, the job will log an error
- The job includes a watchdog. If it hangs during a hardware read, it force-exits and the supervisor restarts it

---

## CAEN High Voltage Supply

The `DAQJobCAENHV` monitors and logs CAEN high-voltage power supply parameters.

### Requirements

- CAEN HV mainframe (SY4527, SY5527, N1470, etc.)
- Network connection to the HV unit

### Configuration

```toml
daq_job_type = "DAQJobCAENHV"

system_type = "SY4527"
link_type = "TCPIP"
connection_arg = "192.168.1.100"
username = ""
password = ""

poll_interval_seconds = 1          # How often to read HV parameters

# Optional: monitor specific channels
[[channels_to_monitor]]
slot = 0
channels = [0, 1, 2, 3]

# Optional: specific parameters
params_to_monitor = ["VMon", "IMon", "V0Set", "Pw", "Status"]

# Storage: CSV snapshot (overwritten each poll)
[store_config.csv]
file_path = "caen_hv.csv"
overwrite = true

# Storage: Redis timeseries (per-parameter)
[timeseries_store_config.redis]
key = "hv"
use_timeseries = true
```

### Available parameters

| Parameter | Description |
|-----------|-------------|
| `VMon` | Measured voltage (V) |
| `IMon` | Measured current (\(\mu\)A) |
| `V0Set` | Set voltage (V) |
| `I0Set` | Current limit (\(\mu\)A) |
| `Pw` | Power status (ON/OFF) |
| `Status` | Channel status code |
| `RUp` | Ramp-up rate (V/s) |
| `RDwn` | Ramp-down rate (V/s) |

### Troubleshooting

- Verify the HV unit is reachable: `ping 192.168.1.100`
- Ensure the `system_type` matches your hardware model exactly
- If authentication fails, leave `username` and `password` empty for unsecured units

---

## CAEN N1081B

The `DAQJobN1081B` polls counter/logic data from the
[CAEN N1081B Four-Fold Programmable Logic Unit](https://caen.it/products/n1081b/)
over a WebSocket connection.

### Requirements

- CAEN N1081B board configured with a static IP
- Network connectivity between the host and the board
- `n1081b-sdk` package (installed via git — see `pyproject.toml`)

### Configuration

```toml
daq_job_type = "DAQJobN1081B"

host = "192.168.1.100"         # IP address of the N1081B board
port = "8080"                   # WebSocket port (default)
password = "admin"              # Login password for the board
sections_to_store = ["SEC_A", "SEC_B"]

[store_config.csv]
file_path = "n1081b.csv"
add_date = true
```

### Troubleshooting

- Verify the board is reachable: `ping 192.168.1.100`
- The WebSocket endpoint is at `ws://<host>:8080/`
- Wrong `password` raises a login error in the logs
- The job retries the connection automatically if the WebSocket drops

---

## Camera (OpenCV)

The `DAQJobCamera` captures images from any camera accessible via OpenCV.

### Requirements

- USB or built-in camera
- `opencv-python` package (included in requirements)

### Configuration

```toml
daq_job_type = "DAQJobCamera"

camera_device_index = 0
store_interval_seconds = 1
store_interval_seconds_no_movement = 5

# Movement detection
movement_detection_threshold = 0.02    # Fraction of pixels that must change
movement_detection_frame_width = 500   # Downscaled frame width for detection

# Time overlay
enable_time_text = true
time_text_position = "TOP_LEFT"        # TOP_LEFT, TOP_RIGHT, BOTTOM_LEFT, BOTTOM_RIGHT

[store_config.raw]
file_path = "camera_capture.jpg"
overwrite = true
```

On Linux, you can also use the device name instead of index:

```toml
camera_device_name = "MyUSB-Camera"  # Searches /dev/v4l/by-id/
```

### Movement detection

When enabled, the camera job captures images more frequently when motion
is detected. It compares consecutive frames and triggers capture when the
fraction of changed pixels exceeds `movement_detection_threshold`.

---

## Xiaomi Mijia BLE Sensor

The `DAQJobXiaomiMijia` reads temperature and humidity from Xiaomi
Bluetooth Low Energy sensors (LYWSD03MMC).

### Requirements

- Xiaomi LYWSD03MMC sensor (with custom firmware for BLE)
- Bluetooth adapter on the host machine
- `lywsd03mmc-client` package (installed via git)

### Configuration

```toml
daq_job_type = "DAQJobXiaomiMijia"

mac_address = "AA:BB:CC:DD:EE:FF"   # Replace with your sensor's MAC
poll_interval_seconds = 10
connect_retries = 5
connect_retry_delay = 2.0

[store_config.csv]
file_path = "xiaomi_mijia.csv"
add_date = true
```

### Finding your sensor's MAC address

Use a BLE scanner on the host machine:

```bash
# Linux
sudo hcitool lescan

# macOS (with Xcode tools)
sudo /usr/sbin/system_profiler SPBluetoothDataType
```

---

## PC Metrics (Built-in)

The `DAQJobPCMetrics` collects CPU usage, memory, and disk metrics from
the host machine.

### Configuration

```toml
daq_job_type = "DAQJobPCMetrics"

poll_interval_seconds = 5

[store_config.csv]
file_path = "pc_metrics.csv"
add_date = true
```

---

## Next steps

- [Deployment](deployment.md) — run multiple machines together
- [Monitoring](../operations/monitoring.md) — check system health
- [Troubleshooting](../operations/troubleshooting.md) — common hardware issues
