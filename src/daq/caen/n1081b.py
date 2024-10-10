import time
from dataclasses import dataclass

from N1081B import N1081B
from websocket import WebSocket

from daq.base import DAQJob
from daq.models import DAQJobMessage
from daq.store.models import StorableDAQJobConfig

N1081B_QUERY_INTERVAL_SECONDS = 1


@dataclass
class DAQN1081BConfig(StorableDAQJobConfig):
    host: str
    port: str
    password: str
    sections_to_store: list[str]


class DAQJobN1081B(DAQJob):
    config_type = DAQN1081BConfig
    device: N1081B
    config: DAQN1081BConfig

    def __init__(self, config: DAQN1081BConfig):
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

    def _poll_sections(self):
        for section in self.config.sections_to_store:
            section = N1081B.Section[section]

            res = self.device.get_function_results(section)
            if not res:
                continue

            data = res["data"]
            if "counters" not in data:
                self._logger.info(f"No counters in section {section}")
                continue

            self._logger.info(f"For section {section}")
            for counter in data["counters"]:
                self._logger.info(f"Lemo {counter['lemo']}: {counter['value']}")

        self._logger.info("===")
