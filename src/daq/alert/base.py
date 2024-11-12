from typing import Any

from daq.alert.models import DAQJobMessageAlert
from daq.base import DAQJob


class DAQJobAlert(DAQJob):
    allowed_message_in_types = [DAQJobMessageAlert]
    _alerts: list[DAQJobMessageAlert]

    def __init__(self, config: Any, **kwargs):
        super().__init__(config, **kwargs)
        self._alerts = []

    def start(self):
        while True:
            self.consume(nowait=False)
            self.alert_loop()
            self._alerts.clear()

    def handle_message(self, message: DAQJobMessageAlert) -> bool:
        self._alerts.append(message)
        return super().handle_message(message)

    def alert_loop(self):
        raise NotImplementedError
