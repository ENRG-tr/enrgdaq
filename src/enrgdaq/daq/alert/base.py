import time
from typing import Any

from enrgdaq.daq.alert.models import DAQJobMessageAlert
from enrgdaq.daq.base import DAQJob

DAQ_JOB_ALERT_LOOP_SLEEP_SECONDS = 1


class DAQJobAlert(DAQJob):
    allowed_message_in_types = [DAQJobMessageAlert]

    def __init__(self, config: Any, **kwargs):
        super().__init__(config, **kwargs)

    def handle_message(self, message: DAQJobMessageAlert) -> bool:
        return super().handle_message(message)

    def start(self):
        while not self._has_been_freed:
            time.sleep(DAQ_JOB_ALERT_LOOP_SLEEP_SECONDS)
