"""
Python demo for CAEN Digitizer


The demo aims to show users how to work with the CAENDigitizer library in Python.
It performs a dummy acquisition using a CAEN Digitizer.
Once connected to the device, the acquisition starts, a software trigger is sent,
and the data are read after stopping the acquisition.
"""

__author__ = "Giovanni Cerretani"
__copyright__ = "Copyright (C) 2025 CAEN SpA"
__license__ = "MIT-0"
# SPDX-License-Identifier: MIT-0
__contact__ = "https://www.caen.it/"

import time
from typing import Literal, Optional

import numpy as np
from caen_libs import caendigitizer as dgtz

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import DAQJobConfig, DAQJobMessage
from enrgdaq.daq.store.models import (
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
    record_length: int = 4096
    channel_enable_mask: int = 1
    channel_trigger_threshold: int = 32768
    channel_self_trigger_channel: int = 0
    channel_self_trigger_mode: dgtz.TriggerMode = dgtz.TriggerMode.ACQ_ONLY  # pyright: ignore[reportPrivateImportUsage]
    sw_trigger_mode: dgtz.TriggerMode = dgtz.TriggerMode.ACQ_ONLY  # pyright: ignore[reportPrivateImportUsage]
    max_num_events_blt: int = 1
    acquisition_mode: dgtz.AcqMode = dgtz.AcqMode.SW_CONTROLLED  # pyright: ignore[reportPrivateImportUsage]
    acquisition_timeout: int = 5
    acquisition_interval_seconds: int = 1
    peak_detection_threshold: Optional[int] = None
    millivolts_per_adc: float = 1.0

    peak_store_config: Optional[DAQJobStoreConfig] = None
    waveform_store_config: Optional[DAQJobStoreConfig] = None


class DAQJobCAENDigitizer(DAQJob):
    """
    DAQJob for CAEN Digitizers.
    """

    config_type = DAQJobCAENDigitizerConfig
    config: DAQJobCAENDigitizerConfig

    def __init__(self, config: DAQJobCAENDigitizerConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._logger.info(
            f"CAEN Digitizer binding loaded (lib version {dgtz.lib.sw_release()})"
        )

    def handle_message(self, message: DAQJobMessage):
        super().handle_message(message)
        return True

    def start(self):
        """
        Main acquisition loop.
        """
        while True:
            self.consume()
            self._acquire()
            time.sleep(self.config.acquisition_interval_seconds)

    def _acquire(self):
        """
        Connects to the digitizer, configures it, and acquires data.
        """
        try:
            with dgtz.Device.open(
                dgtz.ConnectionType[self.config.connection_type],  # pyright: ignore[reportPrivateImportUsage]
                self.config.link_number,
                self.config.conet_node,
                self.config.vme_base_address,
            ) as device:
                self._logger.info("Connected to Digitizer")
                self._run_acquisition(device)
        except Exception as e:
            self._logger.error(f"Error during acquisition: {e}")

    def _run_acquisition(self, device: dgtz.Device):
        """
        Performs a single acquisition run.
        """
        device.reset()
        info = device.get_info()
        self._logger.info(f"  Model Name:        {info.model_name}")
        self._logger.info(f"  Serial Number:     {info.serial_number}")
        self._logger.info(f"  Firmware Code:     {info.firmware_code.name}")

        if info.firmware_code != dgtz.FirmwareCode.STANDARD_FW:  # pyright: ignore[reportPrivateImportUsage]
            raise NotImplementedError("Only STANDARD_FW is supported at the moment.")

        self._configure_device(device)

        device.malloc_readout_buffer()
        device.allocate_event()

        device.sw_start_acquisition()
        device.send_sw_trigger()
        device.read_data(
            dgtz.ReadMode.SLAVE_TERMINATED_READOUT_MBLT,  # pyright: ignore[reportPrivateImportUsage]
        )

        num_events = device.get_num_events()
        self._logger.info(f"Acquired {num_events} events.")

        for i in range(num_events):
            evt_info, buffer = device.get_event_info(i)
            evt = device.decode_event(buffer)
            self._send_store_message(evt, info.channels, i)

            for ch in range(info.channels):
                self._process_signal(evt.data_channel[ch], ch, i)  # pyright: ignore[reportAttributeAccessIssue]

        device.sw_stop_acquisition()

    def _configure_device(self, device: dgtz.Device):
        """
        Configures the digitizer based on the job configuration.
        """
        device.set_record_length(self.config.record_length)
        device.set_channel_enable_mask(self.config.channel_enable_mask)
        device.set_channel_trigger_threshold(
            self.config.channel_self_trigger_channel,
            self.config.channel_trigger_threshold,
        )
        device.set_channel_self_trigger(
            self.config.channel_self_trigger_mode,
            self.config.channel_self_trigger_channel,
        )
        device.set_sw_trigger_mode(self.config.sw_trigger_mode)
        device.set_max_num_events_blt(self.config.max_num_events_blt)
        device.set_acquisition_mode(self.config.acquisition_mode)

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

    def _send_store_message(self, event, num_channels: int, event_id: int):
        """
        Sends the acquired data to the store.
        """
        timestamp = get_now_unix_timestamp_ms()
        keys = ["timestamp", "event_id"]
        values = [timestamp, event_id]

        for ch in range(num_channels):
            keys.append(f"channel_{ch}")
            values.append(event.data_channel[ch])

        assert self.config.waveform_store_config is not None
        self._put_message_out(
            DAQJobMessageStoreTabular(
                store_config=self.config.waveform_store_config,
                tag="waveform",
                keys=keys,
                data=[values],
            )
        )
