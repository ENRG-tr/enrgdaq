import time
from typing import override

from enrgdaq.daq.alert.base import DAQJobMessageAlert
from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import DAQJobMessage
from enrgdaq.daq.store.models import (
    DAQJobMessageStorePyArrow,
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

    @override
    def start(self):
        while not self._has_been_freed:
            time.sleep(1)

    def handle_message(self, message: DAQJobMessage) -> bool:
        if not super().handle_message(message):
            return False
        if not isinstance(message, DAQJobMessageAlert):
            return True

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
                message.daq_job_info.daq_job_type,
                message.alert_info.severity,
                message.alert_info.message,
            ]
        ]

        import pyarrow as pa

        if data_to_send:
            table = pa.table(
                {key: [row[i] for row in data_to_send] for i, key in enumerate(keys)}
            )
        else:
            table = pa.table({key: [] for key in keys})

        self._put_message_out(
            DAQJobMessageStorePyArrow(
                store_config=self.config.store_config,
                table=table,
            )
        )

        return True
