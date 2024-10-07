import logging
import time
from dataclasses import dataclass

from N1081B import N1081B

from daq.models import DAQJob, DAQJobConfig

N1081B_QUERY_INTERVAL_SECONDS = 1

# set logging level to debug
logging.basicConfig(level=logging.DEBUG)


@dataclass
class DAQN1081BConfig(DAQJobConfig):
    host: str
    port: str
    sections_to_store: list[str]


class DAQJobN1081B(DAQJob):
    config_type = DAQN1081BConfig
    device: N1081B
    config: DAQN1081BConfig

    def __init__(self, config: DAQN1081BConfig):
        super().__init__(config)
        self.device = N1081B(f"{config.host}:{config.port}?")

    def start(self):
        self.device.connect()
        success = self.device.login("password")
        if not success:
            raise Exception("Login failed")
        while True:
            if self._should_stop:
                return

            self._loop()
            time.sleep(N1081B_QUERY_INTERVAL_SECONDS)

    def _loop(self):
        for section in self.config.sections_to_store:
            section = N1081B.Section[section]

            res = self.device.get_function_results(section)
            if not res:
                continue

            data = res["data"]
            if "counters" not in data:
                logging.info(f"No counters in section {section}")
                continue

            logging.info(f"For section {section}")
            for counter in data["counters"]:
                logging.info(f"Lemo {counter['lemo']}: {counter['value']}")

        logging.info("===")
