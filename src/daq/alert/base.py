from datetime import datetime
from enum import Enum
from typing import Any

from msgspec import Struct

from daq.base import DAQJob
from daq.models import DAQJobMessage


class DAQAlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class DAQAlertInfo(Struct):
    message: str
    severity: DAQAlertSeverity


class DAQJobMessageAlert(DAQJobMessage):
    date: datetime
    alert_info: DAQAlertInfo


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
