import time

from daq.base import DAQJob
from daq.models import DAQJobMessage
from daq.store.models import DAQJobMessageStore, DAQJobStoreConfig


class DAQJobStore(DAQJob):
    allowed_store_config_types: list[type[DAQJobStoreConfig]]

    def start(self):
        while True:
            self.consume()
            time.sleep(0.5)

    def handle_message(self, message: DAQJobMessage) -> bool:
        if not self.can_store(message):
            raise Exception(f"Invalid message type: {type(message)}")
        return super().handle_message(message)

    def can_store(self, message: DAQJobMessage) -> bool:
        if not isinstance(message, DAQJobMessageStore):
            return False
        is_message_allowed = False
        for allowed_config_type in self.allowed_store_config_types:
            if isinstance(message.store_config, allowed_config_type):
                is_message_allowed = True
        return is_message_allowed
