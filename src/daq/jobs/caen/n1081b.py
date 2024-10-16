import time
from dataclasses import dataclass

from N1081B import N1081B
from websocket import WebSocket

from daq.base import DAQJob
from daq.models import DAQJobMessage
from daq.store.models import DAQJobMessageStore, StorableDAQJobConfig
from utils.time import get_now_unix_timestamp_ms

N1081B_QUERY_INTERVAL_SECONDS = 1
N1081B_WEBSOCKET_TIMEOUT_SECONDS = 5


@dataclass
class DAQJobN1081BConfig(StorableDAQJobConfig):
    host: str
    port: str
    password: str
    sections_to_store: list[str]


class DAQJobN1081B(DAQJob):
    config_type = DAQJobN1081BConfig
    device: N1081B
    config: DAQJobN1081BConfig

    def __init__(self, config: DAQJobN1081BConfig):
        super().__init__(config)
        self.device = N1081B(f"{config.host}:{config.port}?")

        for section in config.sections_to_store:
            if section not in N1081B.Section.__members__:
                raise Exception(f"Invalid section: {section}")

    def handle_message(self, message: DAQJobMessage):
        super().handle_message(message)

        # Do not handle the rest of the messages if the connection is not established
        if not self._is_connected():
            return False

    def start(self):
        while True:
            self.consume()

            # Log in if not connected
            if not self._is_connected():
                self._logger.info("Connecting to the device...")
                self._connect_to_device()
                self._logger.info("Connected!")

            # Poll sections
            self._poll_sections()

            time.sleep(N1081B_QUERY_INTERVAL_SECONDS)

    def _is_connected(self) -> bool:
        return isinstance(self.device.ws, WebSocket) and self.device.ws.connected

    def _connect_to_device(self):
        if not self.device.connect():
            raise Exception("Connection failed")

        if not self.device.login(self.config.password):
            raise Exception("Login failed")

        if isinstance(self.device.ws, WebSocket):
            self.device.ws.settimeout(N1081B_WEBSOCKET_TIMEOUT_SECONDS)
        else:
            raise Exception("Websocket not found")

    def _poll_sections(self):
        for section in self.config.sections_to_store:
            section = N1081B.Section[section]

            res = self.device.get_function_results(section)
            if not res:
                raise Exception("No results")

            data = res["data"]
            if "counters" not in data:
                raise Exception(f"No counters in section {section}")

            self._send_store_message(data, section.name)

    def _send_store_message(self, data: dict, section):
        keys = ["timestamp", *[f"lemo_{x['lemo']}" for x in data["counters"]]]
        values = [
            get_now_unix_timestamp_ms(),  # unix timestamp in milliseconds
            *[x["value"] for x in data["counters"]],
        ]
        self.message_out.put(
            DAQJobMessageStore(
                store_config=self.config.store_config,
                daq_job=self,
                prefix=section,
                keys=keys,
                data=[values],
            )
        )
