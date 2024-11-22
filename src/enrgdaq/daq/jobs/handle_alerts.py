from enrgdaq.daq.alert.base import DAQJobMessageAlert
from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.store.models import (
    DAQJobMessageStoreTabular,
    StorableDAQJobConfig,
)
from enrgdaq.utils.time import get_unix_timestamp_ms


class DAQJobHandleAlertsConfig(StorableDAQJobConfig):
    """
    Configuration class for DAQJobHandleAlerts.
    Inherits from StorableDAQJobConfig.
    """

    pass


class DAQJobHandleAlerts(DAQJob):
    """
    DAQJobHandleAlerts is a job that stores alert messages (DAQJobMessageAlert).
    """

    allowed_message_in_types = [DAQJobMessageAlert]
    config_type = DAQJobHandleAlertsConfig
    config: DAQJobHandleAlertsConfig

    def start(self):
        """
        Starts the job, continuously consuming messages from the queue.
        """
        while True:
            self.consume(nowait=False)

    def handle_message(self, message: DAQJobMessageAlert) -> bool:
        if not super().handle_message(message):
            return False

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
            DAQJobMessageStoreTabular(
                store_config=self.config.store_config,
                keys=keys,
                data=data_to_send,
            )
        )

        return True
