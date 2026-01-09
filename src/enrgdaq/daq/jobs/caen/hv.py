"""
CAEN HV DAQ Job for controlling CAEN High Voltage power supplies.

Uses the caen_libs caenhvwrapper library to communicate with CAEN HV devices.
"""

from datetime import datetime, timedelta
from typing import Literal, Optional

try:
    from caen_libs import caenhvwrapper as hv
except Exception:
    from enum import Enum
    from types import SimpleNamespace

    # Create proper Enum fallbacks for schema generation
    class SystemType(str, Enum):
        SY1527 = "SY1527"
        SY2527 = "SY2527"
        SY4527 = "SY4527"
        SY5527 = "SY5527"
        N568E = "N568E"
        V65XX = "V65XX"
        N1470 = "N1470"
        V8100 = "V8100"
        N568B = "N568B"
        SMARTHV = "SMARTHV"
        NGPS = "NGPS"
        N1068 = "N1068"
        DT55XXE = "DT55XXE"
        DT55XX = "DT55XX"

    class LinkType(str, Enum):
        TCPIP = "TCPIP"
        RS232 = "RS232"
        USB = "USB"
        OPTLINK = "OPTLINK"
        USB_VCP = "USB_VCP"

    class ParamMode(str, Enum):
        RDONLY = "RDONLY"
        WRONLY = "WRONLY"
        RDWR = "RDWR"

    hv = SimpleNamespace()
    hv.SystemType = SystemType
    hv.LinkType = LinkType
    hv.ParamMode = ParamMode
    hv.Device = None


from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import DAQJobConfig, DAQJobMessage
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    DAQJobStoreConfig,
)
from enrgdaq.utils.time import get_now_unix_timestamp_ms, sleep_for

# Poll interval for HV parameters in seconds
CAEN_HV_POLL_INTERVAL_SECONDS = 1


class DAQJobMessageCAENHVSetChParam(DAQJobMessage):
    """
    Message to set a channel parameter on a CAEN HV device.

    Use this message to set voltage (V0Set, V1Set), current limits, or other
    channel parameters on CAEN HV power supplies.

    Common parameter names:
    - V0Set: Set V0 voltage limit (float, in Volts)
    - V1Set: Set V1 voltage limit (float, in Volts)
    - I0Set: Set I0 current limit (float, in microamps)
    - I1Set: Set I1 current limit (float, in microamps)
    - Pw: Power on/off (int, 0=off, 1=on)
    - RUp: Ramp up rate (float, V/s)
    - RDwn: Ramp down rate (float, V/s)

    Note: Available parameters depend on your specific HV board model.
    """

    slot: int
    """The slot number of the board."""

    channel_list: list[int]
    """List of channel numbers to set the parameter on."""

    param_name: str
    """Name of the parameter to set (e.g., 'V0Set', 'V1Set', 'I0Set', 'Pw')."""

    value: int
    """The value to set."""


class DAQJobCAENHVConfig(DAQJobConfig, kw_only=True):
    """
    Configuration for the CAEN HV DAQ Job.

    Connects to a CAEN HV power supply and periodically polls channel parameters.
    """

    system_type: Literal[
        "SY1527",
        "SY2527",
        "SY4527",
        "SY5527",
        "N568E",
        "V65XX",
        "N1470",
        "V8100",
        "N568B",
        "SMARTHV",
        "NGPS",
        "N1068",
        "DT55XXE",
        "DT55XX",
    ]
    """CAEN system type (e.g., 'SY4527', 'N1470', 'DT55XX')."""

    link_type: Literal["TCPIP", "RS232", "USB", "OPTLINK", "USB_VCP"]
    """Connection type to use."""

    connection_arg: str
    """
    Connection argument depending on system_type and link_type.
    For TCPIP: IP address or hostname.
    For USB: Device path or index.
    """

    username: str = ""
    """Username for authentication (if required)."""

    password: str = ""
    """Password for authentication (if required)."""

    poll_interval_seconds: float = CAEN_HV_POLL_INTERVAL_SECONDS
    """Interval in seconds between polling channel parameters."""

    store_config: Optional[DAQJobStoreConfig] = None
    """
    Configuration for storing all channel parameter data in a single dump.
    Data is stored with all parameters in one tabular format.
    Recommended for CSV storage with overwrite=True for a snapshot view.
    """

    timeseries_store_config: Optional[DAQJobStoreConfig] = None
    """
    Configuration for storing channel parameters as timeseries data.
    Each parameter gets its own tag (e.g., 'VMon', 'IMon') with channel
    identifiers as keys (e.g., 'slot_1_channel_7', 'slot_3_channel_0').
    Recommended for Redis timeseries storage.
    
    Example Redis keys: enrgdaq2.hv.VMon.slot_1_channel_7, 
                        enrgdaq2.hv.IMon.slot_3_channel_0, etc.
    """

    channels_to_monitor: Optional[list[dict]] = None
    """
    Optional list of specific channels to monitor.
    Each dict should have 'slot' and 'channels' keys.
    If None, all available channels on all boards will be monitored.
    Example: [{"slot": 0, "channels": [0, 1, 2]}, {"slot": 1, "channels": [0]}]
    """

    params_to_monitor: Optional[list[str]] = None
    """
    Optional list of specific parameter names to monitor.
    If None, all readable parameters will be monitored.
    Example: ["VMon", "IMon", "V0Set", "Pw", "Status"]
    """


class DAQJobCAENHV(DAQJob):
    """
    DAQJob for CAEN High Voltage power supplies.

    This job connects to a CAEN HV device and:
    - Periodically polls channel parameters and stores them as tabular data
    - Accepts messages to set channel parameters (voltage, current limits, etc.)

    Supported devices include SY4527, SY5527, N1470, DT55XX series, and others.
    """

    config_type = DAQJobCAENHVConfig
    config: DAQJobCAENHVConfig
    allowed_message_in_types = [DAQJobMessageCAENHVSetChParam]
    restart_offset = timedelta(seconds=5)
    watchdog_timeout_seconds = 60
    watchdog_force_exit = True  # CAEN library calls can hang, force exit if stuck

    _device: Optional[hv.Device] = None

    def __init__(self, config: DAQJobCAENHVConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._device = None

    def start(self):
        self._logger.info("Opening CAEN HV device...")

        system_type = hv.SystemType[self.config.system_type]
        link_type = hv.LinkType[self.config.link_type]

        try:
            with hv.Device.open(
                system_type,
                link_type,
                self.config.connection_arg,
                self.config.username,
                self.config.password,
            ) as device:
                self._device = device
                self._logger.info(
                    f"Connected to CAEN HV device (lib version {hv.lib.sw_release()})"
                )

                # Initialize internal crate map
                slots = device.get_crate_map()
                self._logger.info(
                    f"Found {len([s for s in slots if s is not None])} board(s)"
                )

                self._run_loop(device, slots)

        except Exception as e:
            self._logger.error(
                f"Error connecting to CAEN HV device: {e}", exc_info=True
            )
            raise

    def _run_loop(self, device: hv.Device, slots):
        """Main loop that polls parameters and handles messages."""
        if self._watchdog.is_enabled:
            self._logger.info(
                f"Watchdog enabled with {self._watchdog.timeout_seconds}s timeout"
            )

        try:
            while not self._watchdog.is_triggered():
                start_time = datetime.now()

                # Start/reset watchdog timer at the beginning of each iteration
                self._watchdog.reset()

                # Poll channel parameters
                try:
                    self._poll_channel_params(device, slots)
                except Exception as e:
                    self._logger.error(
                        f"Error polling channel parameters: {e}", exc_info=True
                    )

                # Sleep for the remaining time of the poll interval
                sleep_for(self.config.poll_interval_seconds, start_time)

            # If we exited due to watchdog, log and let the job end
            if self._watchdog.is_triggered():
                self._logger.warning("Loop exiting due to watchdog timeout")
        finally:
            # Clean up watchdog on exit
            self._watchdog.stop()

    def _poll_channel_params(self, device: hv.Device, slots):
        """Poll channel parameters and send store messages."""
        has_store_config = self.config.store_config is not None
        has_timeseries_config = self.config.timeseries_store_config is not None

        if not has_store_config and not has_timeseries_config:
            return

        timestamp = get_now_unix_timestamp_ms()

        # Per-channel data: {(slot, channel): {param_name: value}}
        channel_params: dict[tuple[int, int], dict[str, float | int | str]] = {}

        for board in slots:
            if board is None:
                continue

            # Check if we should monitor this slot
            if self.config.channels_to_monitor is not None:
                slot_config = next(
                    (
                        c
                        for c in self.config.channels_to_monitor
                        if c.get("slot") == board.slot
                    ),
                    None,
                )
                if slot_config is None:
                    continue
                channels_to_poll = slot_config.get("channels", range(board.n_channel))
            else:
                channels_to_poll = range(board.n_channel)

            for ch in channels_to_poll:
                if ch >= board.n_channel:
                    continue

                try:
                    ch_params = device.get_ch_param_info(board.slot, ch)
                except Exception as e:
                    self._logger.warning(
                        f"Failed to get param info for slot {board.slot} ch {ch}: {e}"
                    )
                    continue

                # Initialize per-channel dict
                channel_params[(board.slot, ch)] = {}

                for param_name in ch_params:
                    # Filter by params_to_monitor if specified
                    if self.config.params_to_monitor is not None:
                        if param_name not in self.config.params_to_monitor:
                            continue

                    try:
                        param_prop = device.get_ch_param_prop(
                            board.slot, ch, param_name
                        )

                        # Skip write-only parameters
                        if param_prop.mode is hv.ParamMode.WRONLY:
                            continue

                        param_value = device.get_ch_param(board.slot, [ch], param_name)
                        # get_ch_param returns a list, get first value
                        value = param_value[0] if param_value else None

                        if value is not None:
                            channel_params[(board.slot, ch)][param_name] = value

                    except Exception as e:
                        self._logger.debug(
                            f"Failed to read {param_name} for slot {board.slot} ch {ch}: {e}"
                        )

        # Build all unique param names for consistent columns
        all_param_names: set[str] = set()
        for params in channel_params.values():
            all_param_names.update(params.keys())
        sorted_param_names = sorted(all_param_names)

        # Send bulk store message (CSV dump) - one row per channel
        if has_store_config and channel_params and sorted_param_names:
            keys = ["timestamp", "slot", "channel", *sorted_param_names]
            data_rows = []

            for (slot, ch), params in channel_params.items():
                # Skip channels with no data
                if not params:
                    continue
                row = [
                    timestamp,
                    slot,
                    ch,
                    *[params.get(pn) for pn in sorted_param_names],
                ]
                data_rows.append(row)

            if data_rows:
                self._put_message_out(
                    DAQJobMessageStoreTabular(
                        store_config=self.config.store_config,
                        tag="ch_param",
                        keys=keys,
                        data=data_rows,
                    )
                )
                self._logger.debug(f"Sent {len(data_rows)} channel parameter readings")

        # Send per-param timeseries messages
        # Structure: tag=param_name, keys=[timestamp, slot_X_channel_Y, ...]
        # This gives Redis keys like: hv.IMon.slot_3_channel_0
        if has_timeseries_config and sorted_param_names:
            # Build sorted channel identifiers
            sorted_channels = sorted(channel_params.keys())
            channel_keys = [f"slot_{s}_channel_{c}" for s, c in sorted_channels]

            for param_name in sorted_param_names:
                # Collect values for this param across all channels
                values_for_param = []
                has_any_value = False

                for slot, ch in sorted_channels:
                    value = channel_params.get((slot, ch), {}).get(param_name)
                    values_for_param.append(value)
                    if value is not None:
                        has_any_value = True

                # Only send if at least one channel has a value for this param
                if not has_any_value:
                    continue

                # Tag = param name, keys = timestamp + channel identifiers
                keys = ["timestamp", *channel_keys]
                values = [timestamp, *values_for_param]

                self._put_message_out(
                    DAQJobMessageStoreTabular(
                        store_config=self.config.timeseries_store_config,
                        tag=param_name,
                        keys=keys,
                        data=[values],
                    )
                )

            self._logger.debug(
                f"Sent timeseries data for {len(sorted_param_names)} params"
            )

    def handle_message(self, message: DAQJobMessage) -> bool:
        """Handle incoming messages."""
        if not super().handle_message(message):
            return False

        if isinstance(message, DAQJobMessageCAENHVSetChParam):
            return self._handle_set_ch_param(message)

        return True

    def _handle_set_ch_param(self, message: DAQJobMessageCAENHVSetChParam) -> bool:
        """Handle a set channel parameter message."""
        if self._device is None:
            self._logger.error("Cannot set parameter: device not connected")
            return True

        try:
            self._logger.info(
                f"Setting {message.param_name}={message.value} on slot {message.slot} "
                f"channels {message.channel_list}"
            )

            self._device.set_ch_param(
                message.slot,
                message.channel_list,
                message.param_name,
                int(message.value),
            )

            self._logger.info(
                f"Successfully set {message.param_name} on slot {message.slot} "
                f"channels {message.channel_list}"
            )
            return True

        except Exception as e:
            self._logger.error(
                f"Failed to set {message.param_name} on slot {message.slot}: {e}",
                exc_info=True,
            )
            return True

    def __del__(self):
        self._logger.info("CAEN HV DAQ job shutting down")
        self._device = None
        return super().__del__()
