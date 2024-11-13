from typing import Any

from daq.alert.models import DAQJobMessageAlert
from daq.base import DAQJob


class DAQJobAlert(DAQJob):
    allowed_message_in_types = [DAQJobMessageAlert]

    def __init__(self, config: Any, **kwargs):
        super().__init__(config, **kwargs)

    def start(self):
        while True:
            self.consume(nowait=False)

    def handle_message(self, message: DAQJobMessageAlert) -> bool:
        return super().handle_message(message)

    def alert_loop(self):
        raise NotImplementedError
