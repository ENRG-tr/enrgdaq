from datetime import datetime

from msgspec import Struct
from zmq import Enum

from enrgdaq.daq.models import DAQJobMessage


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
