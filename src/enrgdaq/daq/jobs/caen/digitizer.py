# pyright: reportPrivateImportUsage=false
import ctypes as ct
import threading
from datetime import timedelta
from typing import Literal, Optional

import caen_libs._caendigitizertypes as _types
from caen_libs import caendigitizer as dgtz
from msgspec import Struct

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import DAQJobConfig
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreRaw,
    DAQJobStoreConfig,
)

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
    sw_trigger_mode: dgtz.TriggerMode = dgtz.TriggerMode.ACQ_ONLY
    max_num_events_blt: int = 1
    acquisition_mode: dgtz.AcqMode = dgtz.AcqMode.SW_CONTROLLED
    acquisition_timeout: int = 5
    acquisition_interval_seconds: int = 1
    peak_detection_threshold: Optional[int] = None
    millivolts_per_adc: float = 1.0

    peak_store_config: Optional[DAQJobStoreConfig] = None
    waveform_store_config: Optional[DAQJobStoreConfig] = None


class DigitizerEvent(Struct, kw_only=True):
    class EventInfo(Struct):
        event_size: int
        board_id: int
        pattern: int
        channel_mask: int
        event_counter: int
        trigger_time_tag: int

    class ChannelWaveform(Struct):
        channel_id: int
        waveform_bytes: bytes
        waveform_dtype: str
        waveform_shape: tuple[int, ...]

    event_info: EventInfo
    waveforms: list[ChannelWaveform]


lib = ct.CDLL("./src/enrgdaq/daq/jobs/caen/digitizer/libdigitizer.so")


CALLBACK_FUNC = ct.CFUNCTYPE(None, ct.POINTER(_types.EventInfoRaw), ct.c_char_p)
dgtz_lib = dgtz.lib


class DAQJobCAENDigitizer(DAQJob):
    """
    DAQJob for CAEN Digitizers using high-performance C library.
    """

    config_type = DAQJobCAENDigitizerConfig
    config: DAQJobCAENDigitizerConfig
    board_info: Optional[dgtz.BoardInfo] = None
    restart_offset = timedelta(seconds=5)

    _msg_buffer: bytearray
    _acquisition_thread: Optional[threading.Thread] = None
    _device: Optional[dgtz.Device] = None

    def __init__(self, config: DAQJobCAENDigitizerConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._msg_buffer = bytearray()
        self._msg_buffer_lock = threading.Lock()

        self._acquisition_thread = None
        self._device = None
        self.ctr = 0
        lib.run_acquisition.argtypes = [ct.c_int, CALLBACK_FUNC]
        lib.stop_acquisition.argtypes = []

        self.callback_delegate = CALLBACK_FUNC(self._event_callback)

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

            lib.run_acquisition(device.handle, self.callback_delegate)
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
                dgtz.TriggerMode.ACQ_ONLY,
                (1 << channel),
            )
        device.set_sw_trigger_mode(self.config.sw_trigger_mode)
        device.set_max_num_events_blt(self.config.max_num_events_blt)
        device.set_acquisition_mode(self.config.acquisition_mode)
        device.set_io_level(dgtz.IOLevel.NIM)
        device.set_post_trigger_size(85)

    def _event_callback(self, event_info_ptr, event_data_ptr):
        pass
        """
        event_info_c = event_info_ptr.contents

        event_info = dgtz.EventInfo(
            event_size=event_info_c.event_size,
            board_id=event_info_c.board_id,
            pattern=event_info_c.pattern,
            channel_mask=event_info_c.channel_mask,
            event_counter=event_info_c.event_counter,
            trigger_time_tag=event_info_c.trigger_time_tag,
        )

        event = cast(
            dgtz.Uint16Event,
            dgtz_lib.decode_event(self.handle, event_data_ptr, self._event_dpp_ptr),
        )

        self._send_store_message(event, event_info)"""

    def _create_event_object(self, event: dgtz.Uint16Event, event_info: dgtz.EventInfo):
        assert self.board_info is not None

        waveforms_to_send = []
        raw_event = event.raw

        for ch in range(self.board_info.channels):
            num_samples = raw_event.ChSize[ch]
            if num_samples == 0:
                continue
            data_ptr = raw_event.DataChannel[ch]
            raw_data_bytes = ct.string_at(data_ptr, num_samples * UINT16_SIZE)

            waveforms_to_send.append(
                DigitizerEvent.ChannelWaveform(
                    channel_id=ch,
                    waveform_bytes=raw_data_bytes,
                    waveform_dtype="<u2",
                    waveform_shape=(num_samples,),
                )
            )

        waveform_data = DigitizerEvent(
            event_info=DigitizerEvent.EventInfo(
                event_size=event_info.event_size,
                board_id=event_info.board_id,
                pattern=event_info.pattern,
                channel_mask=event_info.channel_mask,
                event_counter=event_info.event_counter,
                trigger_time_tag=event_info.trigger_time_tag,
            ),
            waveforms=waveforms_to_send,
        )
        return waveform_data

    def _send_store_message(self, event: dgtz.Uint16Event, event_info: dgtz.EventInfo):
        """
        Sends the acquired data to the store. This method is now called from the C acquisition thread.
        """
        assert self.config.waveform_store_config is not None

        packed_waveforms = self._create_event_object(event, event_info)

        with self._msg_buffer_lock:
            self._encoder.encode_into(packed_waveforms, self._msg_buffer)

            if (
                len(self._msg_buffer)
                > DAQ_JOB_CAEN_DIGITIZER_SEND_EVERY_MB * 1000 * 1000
            ):
                self._put_message_out(
                    DAQJobMessageStoreRaw(
                        store_config=self.config.waveform_store_config,
                        tag="waveform",
                        data=self._msg_buffer,
                    )
                )
                self._msg_buffer.clear()

    def __del__(self):
        self._logger.info("Stopping acquisition...")
        lib.stop_acquisition()
        if self._acquisition_thread:
            self._logger.info("Joining acquisition thread...")
            self._acquisition_thread.join()
        if self._device:
            self._logger.info("Closing Digitizer...")
            self._device.__exit__(None, None, None)
        return super().__del__()
