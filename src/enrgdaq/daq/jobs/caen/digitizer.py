# pyright: reportPrivateImportUsage=false
import ctypes as ct
import os
import subprocess
import threading
from datetime import timedelta
from typing import Literal, Optional

from caen_libs import caendigitizer as dgtz
from msgspec import Struct

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import DAQJobConfig, LogVerbosity
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreRaw,
    DAQJobMessageStoreTabular,
    DAQJobStoreConfig,
)
from enrgdaq.utils.time import get_now_unix_timestamp_ms

DAQ_JOB_CAEN_DIGITIZER_SEND_EVERY_MB = 1
UINT16_SIZE = ct.sizeof(ct.c_uint16)


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
    channel_self_trigger_threshold: int = 32768
    channel_self_trigger_channel_mask: int = 0
    channel_dc_offsets: dict[int, int] = {}
    sw_trigger_mode: dgtz.TriggerMode = dgtz.TriggerMode.ACQ_ONLY
    max_num_events_blt: int = 1
    acquisition_mode: dgtz.AcqMode = dgtz.AcqMode.SW_CONTROLLED
    peak_threshold: int = 750

    waveform_store_config: Optional[DAQJobStoreConfig] = None
    stats_store_config: Optional[DAQJobStoreConfig] = None


DIGITIZER_C_DLL_PATH = "./src/enrgdaq/daq/jobs/caen/digitizer/libdigitizer.so"


WAVEFORM_CALLBACK_FUNC = ct.CFUNCTYPE(None, ct.c_void_p, ct.c_uint32)
STATS_CALLBACK_FUNC = ct.CFUNCTYPE(None, ct.c_void_p)


class RunAcquisitionArgs(ct.Structure):
    _fields_ = [
        ("handle", ct.c_int),
        ("is_debug_verbosity", ct.c_int),
        ("filter_threshold", ct.c_int),
        ("waveform_callback", WAVEFORM_CALLBACK_FUNC),
        ("stats_callback", STATS_CALLBACK_FUNC),
    ]


class AcquisitionStatsRaw(ct.Structure):
    _fields_ = [
        ("acq_events", ct.c_long),
        ("acq_bytes", ct.c_long),
        ("missed_events", ct.c_long),
        ("acq_samples", ct.c_long),
    ]


class AcquisitionStats(Struct):
    acq_events: int
    acq_bytes: int
    missed_events: int
    acq_samples: int

    @classmethod
    def from_raw(cls, raw: AcquisitionStatsRaw):
        return cls(
            acq_events=raw.acq_events,
            acq_bytes=raw.acq_bytes,
            missed_events=raw.missed_events,
            acq_samples=raw.acq_samples,
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

    def start(self):
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

    def _run_acquisition(self, device: dgtz.Device):
        try:
            self.board_info = device.get_info()
            self._configure_device(device, self.board_info)

            args = RunAcquisitionArgs()
            args.handle = device.handle
            args.is_debug_verbosity = self.config.verbosity == LogVerbosity.DEBUG
            args.filter_threshold = self.config.peak_threshold
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
        for channel in range(info.channels):
            device.set_channel_trigger_threshold(
                channel,
                self.config.channel_self_trigger_threshold,
            )
            device.set_trigger_polarity(
                channel,
                dgtz.TriggerPolarity.ON_RISING_EDGE,
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
        device.set_io_level(dgtz.IOLevel.NIM)
        device.set_post_trigger_size(85)
        device.calibrate()

    def _waveform_callback(self, buffer_ptr: ct.c_void_p, buffer_len: int):
        assert (
            self.config.waveform_store_config is not None
        ), "waveform_store_config is None"
        event_data_ptr = ct.cast(buffer_ptr, ct.POINTER(ct.c_uint16))
        event_data_bytes = ct.string_at(event_data_ptr, buffer_len)

        self._put_message_out(
            DAQJobMessageStoreRaw(
                store_config=self.config.waveform_store_config,
                tag="waveform",
                data=event_data_bytes,
            )
        )

    def _stats_callback(self, buffer_ptr: ct.c_void_p):
        assert self.config.stats_store_config is not None, "stats_store_config is None"
        stats_raw = ct.cast(buffer_ptr, ct.POINTER(AcquisitionStatsRaw)).contents
        stats = AcquisitionStats.from_raw(stats_raw)

        self._put_message_out(
            DAQJobMessageStoreTabular(
                store_config=self.config.stats_store_config,
                tag="stats",
                keys=[
                    "timestamp",
                    "acq_events",
                    "acq_bytes",
                    "missed_events",
                    "acq_samples",
                ],
                data=[
                    [
                        get_now_unix_timestamp_ms(),
                        stats.acq_events,
                        stats.acq_bytes,
                        stats.missed_events,
                        stats.acq_samples,
                    ]
                ],
            )
        )

    def __del__(self):
        self._logger.info("Stopping acquisition...")
        self._lib.stop_acquisition()
        if self._device:
            self._logger.info("Closing Digitizer...")
            self._device.__exit__(None, None, None)
        return super().__del__()
