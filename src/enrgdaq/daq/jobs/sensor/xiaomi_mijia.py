import logging
import time

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    StorableDAQJobConfig,
)
from enrgdaq.utils.time import get_now_unix_timestamp_ms

try:
    from lywsd03mmc.lywsd03mmc_client import (  # type: ignore
        Lywsd03mmcClientSyncContext,
        Lywsd03mmcData,
    )
except ImportError:
    Lywsd03mmcClientSyncContext = None  # type: ignore
    Lywsd03mmcData = None  # type: ignore


class DAQXiaomiMijiaConfig(StorableDAQJobConfig):
    """
    Configuration for DAQJobXiaomiMijia.
    Attributes:
        mac_address: Bluetooth MAC address of the sensor.
        poll_interval_seconds: How often to poll the sensor.
        connect_retries: Number of connection attempts before failing.
        connect_retry_delay: Delay (seconds) between connection attempts.
        timeout_sec: Connection timeout in seconds.
    """

    mac_address: str
    poll_interval_seconds: int = 10
    connect_retries: int = 5
    connect_retry_delay: float = 2.0
    timeout_sec: float = 60.0


class DAQJobXiaomiMijia(DAQJob):
    config_type = DAQXiaomiMijiaConfig
    config: DAQXiaomiMijiaConfig

    def __init__(self, config: DAQXiaomiMijiaConfig, **kwargs):
        super().__init__(config, **kwargs)
        logging.getLogger("bleak.backends.winrt.scanner").setLevel(
            self.config.verbosity.to_logging_level()
        )
        logging.getLogger("bleak.backends.winrt.client").setLevel(
            self.config.verbosity.to_logging_level()
        )
        logging.getLogger("asyncio").setLevel(self.config.verbosity.to_logging_level())

    def start(self):
        while not self._has_been_freed:
            try:
                # _get_data now returns (data, units)
                data = self._get_data()
                self._send_store_message(data)
            except Exception as e:
                self._logger.warning(
                    f"Failed to get data: {e}. Retrying after poll interval..."
                )
            time.sleep(self.config.poll_interval_seconds)

    def _get_data(self):
        """Attempts to connect and retrieve data using the context manager."""
        assert (
            Lywsd03mmcClientSyncContext is not None
        ), "lywsd03mmc library not installed"

        for attempt in range(1, self.config.connect_retries + 1):
            try:
                self._logger.debug(
                    f"Connecting to {self.config.mac_address} (attempt {attempt})..."
                )
                # Use the Lywsd03mmcClientSyncContext from your script
                with Lywsd03mmcClientSyncContext(
                    self.config.mac_address, timeout_sec=self.config.timeout_sec
                ) as client:
                    data: Lywsd03mmcData = client.get_data()
                    self._logger.debug("Connected and got data.")
                    return data

            except Exception as e:
                self._logger.warning(f"Connection attempt {attempt} failed: {e}")
                if attempt < self.config.connect_retries:
                    time.sleep(self.config.connect_retry_delay)
                else:
                    self._logger.error("All connection attempts failed.")
                    raise

    def _send_store_message(self, data: Lywsd03mmcData):
        """Sends the formatted data to the store."""

        keys = ["timestamp", "temperature", "humidity", "battery"]
        values = [
            get_now_unix_timestamp_ms(),
            data.temperature,
            data.hum,
            data.battery_percentage,
        ]

        self._logger.debug(f"Sending data to store: {dict(zip(keys, values))}")
        self._put_message_out(
            DAQJobMessageStoreTabular(
                store_config=self.config.store_config,
                keys=keys,
                data=[values],
            )
        )
