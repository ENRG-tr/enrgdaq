import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from daq.base import DAQJob
from daq.models import DAQJobMessage


class DAQJobMessageAlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class DAQJobMessageAlert(DAQJobMessage):
    daq_job: DAQJob
    date: datetime
    message: str
    severity: DAQJobMessageAlertSeverity


class DAQJobAlert(DAQJob):
    allowed_message_in_types = [DAQJobMessageAlert]
    _alerts: list[DAQJobMessageAlert] = []

    def __init__(self, config: Any):
        super().__init__(config)
        self._alerts = []

    def start(self):
        while True:
            self.consume()
            time.sleep(0.5)

    def handle_message(self, message: DAQJobMessageAlert) -> bool:
        self._alerts.append(message)
        return super().handle_message(message)

    def alert_loop(self):
        raise NotImplementedError
