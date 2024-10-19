import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from dataclasses_json import DataClassJsonMixin

from daq.base import DAQJob, DAQJobInfo
from daq.models import DAQJobMessage


class DAQAlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class DAQAlertInfo(DataClassJsonMixin):
    message: str
    severity: DAQAlertSeverity


@dataclass
class DAQJobMessageAlert(DAQJobMessage):
    daq_job_info: DAQJobInfo
    date: datetime
    alert_info: DAQAlertInfo


class DAQJobAlert(DAQJob):
    allowed_message_in_types = [DAQJobMessageAlert]
    _alerts: list[DAQJobMessageAlert]

    def __init__(self, config: Any):
        super().__init__(config)
        self._alerts = []

    def start(self):
        while True:
            self.consume()
            self.alert_loop()
            self._alerts.clear()
            time.sleep(0.5)

    def handle_message(self, message: DAQJobMessageAlert) -> bool:
        self._alerts.append(message)
        return super().handle_message(message)

    def alert_loop(self):
        raise NotImplementedError
