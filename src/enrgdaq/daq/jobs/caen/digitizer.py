# pyright: reportPrivateImportUsage=false
import ctypes as ct
import io
import os
import queue
import subprocess
import sys
import threading
import time
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import Literal, Optional

import numpy as np
import pyarrow as pa

try:
    from caen_libs import caendigitizer as dgtz
except Exception:
    from enum import Enum
    from types import SimpleNamespace

    # Create proper Enum fallbacks for schema generation
    class TriggerMode(int, Enum):
        DISABLED = 0
        ACQ_ONLY = 1
        EXTOUT_ONLY = 2
        ACQ_AND_EXTOUT = 3

    class AcqMode(int, Enum):
        SW_CONTROLLED = 0

    class TriggerPolarity(int, Enum):
        ON_RISING_EDGE = 0
        ON_FALLING_EDGE = 1

    class IOLevel(str, Enum):
        NIM = "NIM"
        TTL = "TTL"

    dgtz = SimpleNamespace()
    dgtz.TriggerMode = TriggerMode
    dgtz.AcqMode = AcqMode
    dgtz.TriggerPolarity = TriggerPolarity
    dgtz.IOLevel = IOLevel
    dgtz.ConnectionType = {}
    dgtz.Device = None
    dgtz.BoardInfo = None
import lz4.frame
from msgspec import Struct

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import (
    DAQJobConfig,
    DAQJobMessage,
    DAQJobMessageStop,
    LogVerbosity,
)
from enrgdaq.daq.store.models import (
    DAQJobMessageStorePyArrow,
    DAQJobStoreConfig,
)
from enrgdaq.utils.time import get_now_unix_timestamp_ms

DAQ_JOB_CAEN_DIGITIZER_SEND_EVERY_MB = 1
UINT16_SIZE = ct.sizeof(ct.c_uint16)


class BaselinePosition(str, Enum):
    """Position of the baseline in the ADC range."""
    TOP = "TOP"        # Baseline at 1023 (max) - signals go negative
    MIDDLE = "MIDDLE"  # Baseline at 512 (center) - signals go both ways
    BOTTOM = "BOTTOM"  # Baseline at 0 (min) - signals go positive

    def get_target_value(self) -> int:
        """Get the target ADC value for this baseline position."""
        if self == BaselinePosition.TOP:
            return 1023
        elif self == BaselinePosition.MIDDLE:
            return 512
        else:  # BOTTOM
            return 0


class DAQJobCAENDigitizerConfig(DAQJobConfig):
    """
    Configuration class for the CAEN Digitizer DAQ Job.
    """

    connection_type: Literal[
        "USB", "PCI_EXPRESS_A4818", "PCI_EXPRESS_A2818", "OPTICAL_LINK"
    ]
    link_number: str
    conet_node: int = 0
    vme_base_address: int = 0
    record_length: int = 1024
    channel_enable_mask: int = 1
    channel_self_trigger_threshold_mv: int = 100  # Threshold in mV from baseline
    channel_self_trigger_channel_mask: int = 0
    trigger_polarity: dgtz.TriggerPolarity = dgtz.TriggerPolarity.ON_RISING_EDGE
    channel_dc_offsets: dict[int, int] = {}
    sw_trigger_mode: dgtz.TriggerMode = dgtz.TriggerMode.ACQ_ONLY
    max_num_events_blt: int = 1
    acquisition_mode: dgtz.AcqMode = dgtz.AcqMode.SW_CONTROLLED
    filter_threshold_mv: int = 50  # Keep samples deviating more than this from baseline
    io_level: dgtz.IOLevel = dgtz.IOLevel.NIM
    post_trigger_size: int = 80

    waveform_store_config: Optional[DAQJobStoreConfig] = None
    stats_store_config: Optional[DAQJobStoreConfig] = None

    baseline_position: BaselinePosition = BaselinePosition.TOP

    save_npy_lz4: bool = False
    output_filename: Optional[str] = None


DIGITIZER_C_DLL_PATH = "./src/enrgdaq/daq/jobs/caen/digitizer/libdigitizer.so"


WAVEFORM_CALLBACK_FUNC = ct.CFUNCTYPE(None, ct.c_void_p)
STATS_CALLBACK_FUNC = ct.CFUNCTYPE(None, ct.c_void_p)


class RunAcquisitionArgs(ct.Structure):
    _fields_ = [
        ("handle", ct.c_int),
        ("is_debug_verbosity", ct.c_int),
        ("filter_threshold", ct.c_int),
        ("calibration_target_baseline", ct.c_int),
        ("waveform_callback", WAVEFORM_CALLBACK_FUNC),
        ("stats_callback", STATS_CALLBACK_FUNC),
        ("channel_dc_offsets", ct.c_void_p),
    ]


class WaveformSamplesRaw(ct.Structure):
    _fields_ = [
        ("len", ct.c_uint32),
        # ("pc_unix_ms_timestamp", ct.POINTER(ct.c_uint64)),
        ("real_ns_timestamp", ct.POINTER(ct.c_uint64)),
        ("event_counter", ct.POINTER(ct.c_uint32)),
        # ("trigger_time_tag", ct.POINTER(ct.c_uint32)),
        ("channel", ct.POINTER(ct.c_uint8)),
        ("sample_index", ct.POINTER(ct.c_uint16)),
        # ("value_lsb", ct.POINTER(ct.c_uint16)),
        ("value_mv", ct.POINTER(ct.c_int16)),
    ]


class AcquisitionStatsRaw(ct.Structure):
    _fields_ = [
        # Basic counts
        ("acq_events", ct.c_long),
        ("acq_samples", ct.c_long),
        # Value statistics (in mV)
        ("sum_value_mv", ct.c_long),
        ("sum_value_mv_squared", ct.c_long),
        ("min_value_mv", ct.c_int16),
        ("max_value_mv", ct.c_int16),
        # Filter statistics
        ("samples_filtered_out", ct.c_long),
        ("total_samples_raw", ct.c_long),
        # Performance statistics
        ("events_dropped", ct.c_long),
        ("queue_depth", ct.c_long),
        ("processing_time_us", ct.c_long),
        ("buffer_flush_count", ct.c_long),
    ]


class AcquisitionStats(Struct):
    # Basic counts
    acq_events: int
    acq_samples: int
    # Value statistics (in mV)
    mean_value_mv: float  # Computed from sum_value_mv / acq_samples
    sum_value_mv_squared: int
    min_value_mv: int
    max_value_mv: int
    # Filter statistics
    samples_filtered_out: int
    total_samples_raw: int
    # Performance statistics
    events_dropped: int
    queue_depth: int
    processing_time_us: int
    buffer_flush_count: int

    @classmethod
    def from_raw(cls, raw: AcquisitionStatsRaw):
        mean_value_mv = 0.0
        if raw.acq_samples > 0:
            mean_value_mv = raw.sum_value_mv / raw.acq_samples
        
        return cls(
            acq_events=raw.acq_events,
            acq_samples=raw.acq_samples,
            mean_value_mv=mean_value_mv,
            sum_value_mv_squared=raw.sum_value_mv_squared,
            min_value_mv=raw.min_value_mv,
            max_value_mv=raw.max_value_mv,
            samples_filtered_out=raw.samples_filtered_out,
            total_samples_raw=raw.total_samples_raw,
            events_dropped=raw.events_dropped,
            queue_depth=raw.queue_depth,
            processing_time_us=raw.processing_time_us,
            buffer_flush_count=raw.buffer_flush_count,
        )


class DAQJobCAENDigitizer(DAQJob):
    """
    DAQJob for CAEN Digitizers using high-performance C library.
    """

    config_type = DAQJobCAENDigitizerConfig
    config: DAQJobCAENDigitizerConfig
    board_info: Optional[dgtz.BoardInfo] = None
    restart_offset = timedelta(seconds=5)

    _msg_buffer: bytearray
    _device: Optional[dgtz.Device] = None

    def __init__(self, config: DAQJobCAENDigitizerConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._msg_buffer = bytearray()
        self._msg_buffer_lock = threading.Lock()

        self._writer_queue = None
        self._writer_thread = None
        if self.config.save_npy_lz4:
            assert (
                self.config.output_filename is not None
            ), "output_filename is required when save_npy_lz4 is True"
            os.makedirs(os.path.dirname(self.config.output_filename), exist_ok=True)
            self._writer_queue = queue.Queue()
            self._writer_thread = threading.Thread(
                target=self._writer_thread_func, daemon=True
            )

        self._device = None
        self.ctr = 0

        # Compile if .so does not exist
        if True or not os.path.exists(DIGITIZER_C_DLL_PATH):
            self._logger.info("Compiling C library...")
            ret = subprocess.run(
                ["make", "-C", os.path.dirname(DIGITIZER_C_DLL_PATH)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if ret.returncode != 0:
                self._logger.error(
                    f"Failed to compile with error code: {ret.returncode}"
                )
                self._logger.error(f"stdout: {ret.stdout}")
                self._logger.error(f"stderr: {ret.stderr}")
        self._lib = ct.CDLL(DIGITIZER_C_DLL_PATH)
        self._lib.run_acquisition.argtypes = [ct.c_void_p]
        self._lib.stop_acquisition.argtypes = []

        self._waveform_callback_delegate = WAVEFORM_CALLBACK_FUNC(
            self._waveform_callback
        )
        self._stats_callback_delegate = STATS_CALLBACK_FUNC(self._stats_callback)
        self._converting_npy_lz4 = False

    def start(self):
        if self._writer_thread:
            self._writer_thread.start()

        try:
            self._logger.info("Opening Digitizer...")
            with dgtz.Device.open(
                dgtz.ConnectionType[self.config.connection_type],
                self.config.link_number,
                self.config.conet_node,
                self.config.vme_base_address,
            ) as device:
                self._device = device
                self._logger.info("Connected to Digitizer")
                self._run_acquisition(device)
        except Exception as e:
            self._logger.error(f"Error during acquisition: {e}", exc_info=True)

    def handle_message(self, message: DAQJobMessage) -> bool:
        if not super().handle_message(message):
            return False
        if not isinstance(message, DAQJobMessageStop):
            return True
        if not self.config.save_npy_lz4 or self._converting_npy_lz4:
            return True

        self._converting_npy_lz4 = True
        # Convert npy.lz4 to root
        assert self.config.output_filename is not None

        npy_path = Path(self.config.output_filename)
        if not npy_path.exists():
            self._logger.warning(
                f"Npy file does not exist: {npy_path}, skipping conversion"
            )
            return True

        root_path = npy_path.parent / (npy_path.stem + ".root")
        self._logger.info(f"Running npy2root: {npy_path} -> {root_path}")
        subprocess.run(
            [
                sys.executable,
                "-m",
                "enrgdaq.tools.npy2root",
                str(npy_path),
                str(root_path),
            ],
            check=True,
        )

        return True

    def _run_acquisition(self, device: dgtz.Device):
        try:
            self.board_info = device.get_info()
            self._configure_device(device, self.board_info)

            args = RunAcquisitionArgs()
            args.handle = device.handle
            args.is_debug_verbosity = self.config.verbosity == LogVerbosity.DEBUG
            args.filter_threshold = self.config.filter_threshold_mv
            args.calibration_target_baseline = self.config.baseline_position.get_target_value()
            args.waveform_callback = self._waveform_callback_delegate
            args.stats_callback = self._stats_callback_delegate

            self._lib.run_acquisition(ct.pointer(args))
        except Exception as e:
            self._logger.error(
                f"Error during C-based acquisition setup: {e}", exc_info=True
            )

    def _configure_device(self, device: dgtz.Device, info: dgtz.BoardInfo):
        """
        Configures the digitizer based on the job configuration.
        """
        device.reset()
        device.set_record_length(self.config.record_length)
        device.set_channel_enable_mask(self.config.channel_enable_mask)

        # Convert mV threshold to LSB using target baseline
        target_baseline = self.config.baseline_position.get_target_value()
        # For rising edge: baseline + mV, for falling edge: baseline - mV
        if self.config.trigger_polarity == dgtz.TriggerPolarity.ON_RISING_EDGE:
            trigger_threshold_lsb = target_baseline + (self.config.channel_self_trigger_threshold_mv * 1024 // 1000)
        else:
            trigger_threshold_lsb = target_baseline - (self.config.channel_self_trigger_threshold_mv * 1024 // 1000)
        # Clamp to valid range (10-bit ADC: 0-1023)
        trigger_threshold_lsb = max(0, min(1023, trigger_threshold_lsb))

        for channel in range(info.channels):
            device.set_channel_trigger_threshold(
                channel,
                trigger_threshold_lsb,
            )
            device.set_trigger_polarity(
                channel,
                self.config.trigger_polarity,
            )
            device.set_channel_self_trigger(
                dgtz.TriggerMode.ACQ_ONLY
                if (self.config.channel_self_trigger_channel_mask & (1 << channel))
                else dgtz.TriggerMode.DISABLED,
                (1 << channel),
            )
            if channel in self.config.channel_dc_offsets:
                device.set_channel_dc_offset(
                    channel, self.config.channel_dc_offsets[channel]
                )
        device.set_sw_trigger_mode(self.config.sw_trigger_mode)
        device.set_max_num_events_blt(self.config.max_num_events_blt)
        device.set_acquisition_mode(self.config.acquisition_mode)
        device.set_io_level(self.config.io_level)
        device.set_post_trigger_size(self.config.post_trigger_size)
        device.calibrate()

    def _waveform_callback(self, buffer_ptr: ct.c_void_p):
        assert (
            self.config.waveform_store_config is not None or self.config.save_npy_lz4
        ), "waveform_store_config is None and save_npy_lz4 is False"
        waveform_ptr = ct.cast(buffer_ptr, ct.POINTER(WaveformSamplesRaw)).contents

        keys_to_send = [
            field[0] for field in WaveformSamplesRaw._fields_ if field[0] != "len"
        ]
        data_columns = {}

        for field in keys_to_send:
            data_columns[field] = np.ctypeslib.as_array(
                getattr(waveform_ptr, field), shape=(waveform_ptr.len,)
            ).copy()

        if self.config.save_npy_lz4:
            assert self._writer_queue is not None

            if self._writer_queue.qsize() > 100:
                self._logger.warning(
                    f"Writer queue size high: {self._writer_queue.qsize()}"
                )

            self._writer_queue.put_nowait(data_columns)
            return

        assert self.config.waveform_store_config is not None

        # Create PyArrow table from numpy arrays
        table = pa.table(data_columns)

        self._put_message_out(
            DAQJobMessageStorePyArrow(
                store_config=self.config.waveform_store_config,
                tag="waveform",
                table=table,
            ),
            use_shm=False,
        )

    def _writer_thread_func(self):
        assert (
            self.config.output_filename is not None and self._writer_queue is not None
        )

        while True:
            try:
                data = self._writer_queue.get()
                if data is None:
                    self._writer_queue.task_done()
                    break

                start_time = time.time()
                self._save_waveform_to_npy_lz4(data, self.config.output_filename)

                # Only log debug if it takes significant time to avoid log spam
                elapsed = time.time() - start_time
                if elapsed > 0.001:
                    self._logger.debug(
                        f"Took {elapsed:.6f} seconds to write to the npy.lz4 file"
                    )

                self._writer_queue.task_done()
            except Exception as e:
                self._logger.error(f"Error in writer thread: {e}", exc_info=True)

    def _stats_callback(self, buffer_ptr: ct.c_void_p):
        assert self.config.stats_store_config is not None, "stats_store_config is None"
        stats_raw = ct.cast(buffer_ptr, ct.POINTER(AcquisitionStatsRaw)).contents
        stats = AcquisitionStats.from_raw(stats_raw)

        # Create PyArrow table for stats
        table = pa.table(
            {
                "timestamp": [get_now_unix_timestamp_ms()],
                # Basic counts
                "acq_events": [stats.acq_events],
                "acq_samples": [stats.acq_samples],
                # Value statistics (in mV)
                "mean_value_mv": [stats.mean_value_mv],
                "sum_value_mv_squared": [stats.sum_value_mv_squared],
                "min_value_mv": [stats.min_value_mv],
                "max_value_mv": [stats.max_value_mv],
                # Filter statistics
                "samples_filtered_out": [stats.samples_filtered_out],
                "total_samples_raw": [stats.total_samples_raw],
                # Performance statistics
                "events_dropped": [stats.events_dropped],
                "queue_depth": [stats.queue_depth],
                "processing_time_us": [stats.processing_time_us],
                "buffer_flush_count": [stats.buffer_flush_count],
            }
        )

        self._put_message_out(
            DAQJobMessageStorePyArrow(
                store_config=self.config.stats_store_config,
                tag="stats",
                table=table,
            )
        )

    def _stop_acquisition(self):
        if self._writer_thread and self._writer_queue:
            self._logger.info("Stopping writer thread...")
            self._writer_queue.put(None)
            self._writer_thread.join()
        self._logger.info("Stopping acquisition...")
        if self._lib:
            self._lib.stop_acquisition()
        if self._device:
            self._logger.info("Closing Digitizer...")
            self._device.__exit__(None, None, None)

    def __del__(self):
        self._stop_acquisition()
        return super().__del__()

    def _save_waveform_to_npy_lz4(self, data_columns: dict, path: str):
        """
        Saves waveform data by appending to a lz4 compressed npy file.
        """
        # Save the numpy array to a in-memory buffer
        with io.BytesIO() as bio:
            np.save(bio, data_columns)
            frame = bio.getvalue()

        # Compress the frame
        compressed = lz4.frame.compress(frame, compression_level=-1)

        # Append the compressed frame to the file
        with open(path, "ab") as f:
            # Write the size of the compressed frame first
            f.write(len(compressed).to_bytes(4, "little"))
            # Write the compressed frame
            f.write(compressed)
