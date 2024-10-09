import time
from dataclasses import dataclass

from N1081B import N1081B
from websocket import WebSocket

from daq.models import DAQJob, DAQJobConfig

N1081B_QUERY_INTERVAL_SECONDS = 1


@dataclass
class DAQN1081BConfig(DAQJobConfig):
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

    def start(self):
        while True:
            # Try to connect to the device
            if not self._try_connect():
                self.logger.error("Connection failed, retrying")
                continue

            self._start_loop()

    def _start_loop(self):
        while True:
            if self._should_stop:
                return True

            # Stop if the connection is dropped
            if isinstance(self.device.ws, WebSocket) and not self.device.ws.connected:
                self.logger.error("Connection dropped")
                break

            try:
                self._loop()
            except ConnectionResetError:
                self.logger.error("Connection reset")
                break
            except ConnectionAbortedError:
                self.logger.error("Connection aborted")
                break

            time.sleep(N1081B_QUERY_INTERVAL_SECONDS)

    def _try_connect(self) -> bool:
        try:
            if not self.device.connect():
                return False
        except ConnectionRefusedError:
            return False

        if not self.device.login(self.config.password):
            raise Exception("Login failed")

        return True

    def _loop(self):
        for section in self.config.sections_to_store:
            section = N1081B.Section[section]

            res = self.device.get_function_results(section)
            if not res:
                continue

            data = res["data"]
            if "counters" not in data:
                self.logger.info(f"No counters in section {section}")
                continue

            self.logger.info(f"For section {section}")
            for counter in data["counters"]:
                self.logger.info(f"Lemo {counter['lemo']}: {counter['value']}")

        self.logger.info("===")
