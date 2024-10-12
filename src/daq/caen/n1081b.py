import time
from dataclasses import dataclass

from N1081B import N1081B
from websocket import WebSocket

from daq.base import DAQJob
from daq.models import DAQJobMessage
from daq.store.models import DAQJobMessageStore, StorableDAQJobConfig

N1081B_QUERY_INTERVAL_SECONDS = 1


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

            self._send_store_message(data)

    def _send_store_message(self, data: dict):
        self.message_out.put(
            DAQJobMessageStore(
                store_config=self.config.store_config,
                daq_job=self,
                keys=[f"lemo_{x['lemo']}" for x in data["counters"]],
                data=[[x["value"] for x in data["counters"]]],
            )
        )
