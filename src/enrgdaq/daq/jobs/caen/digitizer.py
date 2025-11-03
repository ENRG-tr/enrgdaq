# pyright: reportPrivateImportUsage=false
import time
from typing import Literal, Optional, cast

import msgspec
import numpy as np
from caen_libs import caendigitizer as dgtz
from msgspec import Struct

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import DAQJobConfig, DAQJobMessage
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreRaw,
    DAQJobMessageStoreTabular,
    DAQJobStoreConfig,
)
from enrgdaq.utils.time import get_now_unix_timestamp_ms


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
    channel_trigger_threshold: int = 32768
    channel_trigger_channel_mask: int = 0
    channel_self_trigger_mode: dgtz.TriggerMode = dgtz.TriggerMode.ACQ_ONLY
    sw_trigger_mode: dgtz.TriggerMode = dgtz.TriggerMode.ACQ_ONLY
    max_num_events_blt: int = 1
    acquisition_mode: dgtz.AcqMode = dgtz.AcqMode.SW_CONTROLLED
    acquisition_timeout: int = 5
    acquisition_interval_seconds: int = 1
    peak_detection_threshold: Optional[int] = None
    millivolts_per_adc: float = 1.0

    peak_store_config: Optional[DAQJobStoreConfig] = None
    waveform_store_config: Optional[DAQJobStoreConfig] = None


class WaveformData(Struct, kw_only=True):
    class Channel(Struct):
        channel_id: int
        waveform: bytes

    waveforms: list[Channel]


class DAQJobCAENDigitizer(DAQJob):
    """
    DAQJob for CAEN Digitizers.
    """

    config_type = DAQJobCAENDigitizerConfig
    config: DAQJobCAENDigitizerConfig
    board_info: Optional[dgtz.BoardInfo] = None

    def __init__(self, config: DAQJobCAENDigitizerConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._logger.info(
            f"CAEN Digitizer binding loaded (lib version {dgtz.lib.sw_release()})"
        )

    def handle_message(self, message: DAQJobMessage):
        super().handle_message(message)
        return True

    def start(self):
        try:
            with dgtz.Device.open(
                dgtz.ConnectionType[self.config.connection_type],
                self.config.link_number,
                self.config.conet_node,
                self.config.vme_base_address,
            ) as device:
                self._logger.info("Connected to Digitizer")
                self._run_acquisition(device)
        except Exception as e:
            self._logger.error(f"Error during acquisition: {e}", exc_info=True)

    def _run_acquisition(self, device: dgtz.Device):
        """
        Performs a single acquisition run.
        """
        device.reset()
        self.board_info = device.get_info()
        self._logger.info(f"  Model Name:        {self.board_info.model_name}")
        self._logger.info(f"  Serial Number:     {self.board_info.serial_number}")
        self._logger.info(f"  Firmware Code:     {self.board_info.firmware_code.name}")

        if self.board_info.firmware_code != dgtz.FirmwareCode.STANDARD_FW:
            raise NotImplementedError("Only STANDARD_FW is supported at the moment.")

        self._configure_device(device, self.board_info)

        device.malloc_readout_buffer()
        device.allocate_event()

        device.sw_start_acquisition()
        last_event_counter = 0
        while True:
            device.read_data(
                dgtz.ReadMode.SLAVE_TERMINATED_READOUT_MBLT,
            )

            num_events = device.get_num_events()
            self._logger.info(f"Acquired {num_events} events.")

            start_time = time.time()
            for i in range(num_events):
                event_info, buffer = device.get_event_info(i)
                event = cast(dgtz.Uint16Event, device.decode_event(buffer))
                self._send_store_message(event, event_info)
                if i == 0:
                    print(
                        "missed",
                        str(event_info.event_counter - last_event_counter),
                        "events",
                    )
                    last_event_counter = event_info.event_counter

                # for ch in range(self.board_info.channels):
                #    self._process_signal(event.data_channel[ch], ch, i)
            end_time = time.time()
            # print("took " + (end_time - start_time).__str__() + " seconds")

    def _configure_device(self, device: dgtz.Device, info: dgtz.BoardInfo):
        """
        Configures the digitizer based on the job configuration.
        """
        device.reset()
        device.set_record_length(self.config.record_length)
        print(self.config.channel_enable_mask)
        device.set_channel_enable_mask(self.config.channel_enable_mask)
        for channel in range(info.channels):
            device.set_channel_trigger_threshold(
                channel,
                self.config.channel_trigger_threshold,
            )
            device.set_trigger_polarity(
                channel,
                dgtz.TriggerPolarity.ON_RISING_EDGE,
            )
            device.set_channel_self_trigger(
                self.config.channel_self_trigger_mode,
                (1 << channel),
            )
        device.set_sw_trigger_mode(self.config.sw_trigger_mode)
        device.set_max_num_events_blt(self.config.max_num_events_blt)
        device.set_acquisition_mode(self.config.acquisition_mode)
        device.set_io_level(dgtz.IOLevel.NIM)
        device.set_post_trigger_size(85)

    def _process_signal(self, signal: np.ndarray, channel: int, event_id: int):
        if self.config.peak_detection_threshold is None:
            return

        if np.any(signal > self.config.peak_detection_threshold):
            peak_adc = np.max(signal)
            peak_mv = peak_adc * self.config.millivolts_per_adc
            self._send_peak_message(peak_mv, channel, event_id)

    def _send_peak_message(self, peak_mv: float, channel: int, event_id: int):
        """
        Sends the detected peak information to the store.
        """
        timestamp = get_now_unix_timestamp_ms()
        keys = ["timestamp", "event_id", "channel", "peak_mv"]
        values = [timestamp, event_id, channel, peak_mv]

        assert self.config.peak_store_config is not None
        self._put_message_out(
            DAQJobMessageStoreTabular(
                store_config=self.config.peak_store_config,
                tag="peak",
                keys=keys,
                data=[values],
            )
        )

    def _pack_waveforms(self, event: dgtz.Uint16Event, event_info: dgtz.EventInfo):
        assert self.board_info is not None

        waveform_data = WaveformData(
            waveforms=[
                WaveformData.Channel(
                    channel_id=ch, waveform=event.data_channel[ch].tolist()
                )
                for ch in range(self.board_info.channels)
            ]
        )
        return msgspec.msgpack.encode(waveform_data)

    def _send_store_message(self, event: dgtz.Uint16Event, event_info: dgtz.EventInfo):
        """
        Sends the acquired data to the store.
        """
        assert self.config.waveform_store_config is not None
        start_time = time.time()
        data = self._pack_waveforms(event, event_info)
        # print("packing took " + (time.time() - start_time).__str__() + " seconds")
        start_time = time.time()
        self._put_message_out(
            DAQJobMessageStoreRaw(
                store_config=self.config.waveform_store_config,
                tag="waveform",
                data=data,
            )
        )
        # print("sending took " + (time.time() - start_time).__str__() + " seconds")
