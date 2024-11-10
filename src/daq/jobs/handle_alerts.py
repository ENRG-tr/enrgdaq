from daq.alert.base import DAQJobMessageAlert
from daq.base import DAQJob
from daq.store.models import DAQJobMessageStore, StorableDAQJobConfig
from utils.time import get_unix_timestamp_ms


class DAQJobHandleAlertsConfig(StorableDAQJobConfig):
    pass


class DAQJobHandleAlerts(DAQJob):
    allowed_message_in_types = [DAQJobMessageAlert]
    config_type = DAQJobHandleAlertsConfig
    config: DAQJobHandleAlertsConfig

    def start(self):
        while True:
            self.consume(nowait=False)

    def handle_message(self, message: DAQJobMessageAlert) -> bool:
        super().handle_message(message)

        keys = [
            "timestamp",
            "daq_job",
            "severity",
            "message",
        ]

        assert message.daq_job_info is not None
        data_to_send = [
            [
                get_unix_timestamp_ms(message.date),
                message.daq_job_info.daq_job_class_name,
                message.alert_info.severity,
                message.alert_info.message,
            ]
        ]

        self._put_message_out(
            DAQJobMessageStore(
                store_config=self.config.store_config,
                keys=keys,
                data=data_to_send,
            )
        )

        return True
