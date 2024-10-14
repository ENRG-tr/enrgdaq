import time
from dataclasses import dataclass

from daq.alert.base import DAQJobMessageAlert
from daq.base import DAQJob
from daq.store.models import DAQJobMessageStore, StorableDAQJobConfig
from utils.time import get_unix_timestamp_ms


@dataclass
class DAQJobHandleAlertsConfig(StorableDAQJobConfig):
    pass


class DAQJobHandleAlerts(DAQJob):
    allowed_message_in_types = [DAQJobMessageAlert]
    config_type = DAQJobHandleAlertsConfig
    config: DAQJobHandleAlertsConfig

    def start(self):
        while True:
            self.consume()
            time.sleep(0.5)

    def handle_message(self, message: DAQJobMessageAlert) -> bool:
        super().handle_message(message)

        keys = [
            "timestamp",
            "daq_job",
            "severity",
            "message",
        ]

        data_to_send = [
            [
                get_unix_timestamp_ms(message.date),
                type(message.daq_job).__name__,
                message.alert_info.severity,
                message.alert_info.message,
            ]
        ]

        self.message_out.put(
            DAQJobMessageStore(
                store_config=self.config.store_config,
                daq_job=self,
                keys=keys,
                data=data_to_send,
            )
        )

        return True
