import time
from typing import Any, Optional

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    StorableDAQJobConfig,
)
from enrgdaq.utils.time import get_now_unix_timestamp_ms

try:
    from lywsd03mmc import Lywsd03mmcClient  # type: ignore
except ImportError:
    Lywsd03mmcClient = None


class DAQXiaomiMijiaConfig(StorableDAQJobConfig):
    """
    Configuration for DAQJobLywsd03mmc.
    Attributes:
        mac_address: Bluetooth MAC address of the sensor.
        poll_interval_seconds: How often to poll the sensor.
        connect_retries: Number of connection attempts before failing.
        connect_retry_delay: Delay (seconds) between connection attempts.
    """

    mac_address: str
    poll_interval_seconds: int = 10
    connect_retries: int = 5
    connect_retry_delay: float = 2.0


class DAQJobXiaomiMijia(DAQJob):
    config_type = DAQXiaomiMijiaConfig
    config: DAQXiaomiMijiaConfig
    _client: Optional[Any]

    def __init__(self, config: DAQXiaomiMijiaConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._client = None

    def start(self):
        self._connect_with_retries()
        while True:
            self.consume()
            try:
                data = self._client.data
                self._send_store_message(data)
            except Exception as e:
                self._logger.warning(f"Failed to get data: {e}. Reconnecting...")
                self._connect_with_retries()
            time.sleep(self.config.poll_interval_seconds)

    def _connect_with_retries(self):
        for attempt in range(1, self.config.connect_retries + 1):
            try:
                self._logger.info(
                    f"Connecting to {self.config.mac_address} (attempt {attempt})..."
                )
                self._client = Lywsd03mmcClient(self.config.mac_address)
                # Try to access data to ensure connection
                _ = self._client.data
                self._logger.info("Connected to sensor.")
                return
            except Exception as e:
                self._logger.warning(f"Connection attempt {attempt} failed: {e}")
                if attempt < self.config.connect_retries:
                    time.sleep(self.config.connect_retry_delay)
                else:
                    self._logger.error("All connection attempts failed.")
                    raise

    def _send_store_message(self, data):
        keys = ["timestamp", "temperature", "humidity", "battery"]
        values = [
            get_now_unix_timestamp_ms(),
            data.temperature,
            data.humidity,
            data.battery,
            getattr(self._client, "units", None),
        ]
        self._put_message_out(
            DAQJobMessageStoreTabular(
                store_config=self.config.store_config,
                keys=keys,
                data=[values],
            )
        )
