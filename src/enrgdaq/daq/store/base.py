import time

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import DAQJobMessage
from enrgdaq.daq.store.models import DAQJobMessageStore

STORE_LOOP_INTERVAL_SECONDS = 0.1


class DAQJobStore(DAQJob):
    allowed_store_config_types: list

    def start(self):
        while True:
            self.consume()
            self.store_loop()
            time.sleep(STORE_LOOP_INTERVAL_SECONDS)

    def store_loop(self):
        raise NotImplementedError

    def handle_message(self, message: DAQJobMessage) -> bool:
        if not self.can_store(message):
            raise Exception(
                f"Invalid message type '{type(message)}' for DAQJob '{type(self).__name__}'"
            )
        return super().handle_message(message)

    def can_store(self, message: DAQJobMessage) -> bool:
        if not isinstance(message, DAQJobMessageStore):
            return False
        is_message_allowed = False
        for allowed_config_type in self.allowed_store_config_types:
            if message.store_config.has_store_config(allowed_config_type):
                is_message_allowed = True
        return is_message_allowed
