import time
from dataclasses import dataclass

from daq.models import DAQJob, DAQJobMessage


class DAQJobStore(DAQJob):
    allowed_message_types: list[type["DAQJobMessageStore"]]

    def start(self):
        while True:
            self.consume()
            time.sleep(0.5)

    def handle_message(self, message: DAQJobMessage) -> bool:
        is_message_allowed = False
        for allowed_message_type in self.allowed_message_types:
            if isinstance(message, allowed_message_type):
                is_message_allowed = True
        if not is_message_allowed:
            raise Exception(f"Invalid message type: {type(message)}")
        return super().handle_message(message)


@dataclass
class DAQJobMessageStore(DAQJobMessage):
    daq_job: DAQJob
