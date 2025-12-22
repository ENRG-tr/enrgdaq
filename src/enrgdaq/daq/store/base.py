import time

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import DAQJobMessage, DAQJobMessageStop
from enrgdaq.daq.store.models import DAQJobMessageStore

# Sleep interval when no messages are available (prevents busy-waiting)
STORE_IDLE_SLEEP_SECONDS = 0.001


class DAQJobStore(DAQJob):
    """
    DAQJobStore is an abstract base class for data acquisition job stores.

    Optimized for high throughput - only sleeps when idle to avoid
    unnecessary latency in message processing.
    """

    allowed_store_config_types: list

    def start(self):
        """
        Starts the continuous loop for consuming and storing data.
        """
        while True:
            messages_processed = self.consume_all()
            self.store_loop()

            if not messages_processed:
                time.sleep(STORE_IDLE_SLEEP_SECONDS)

    def store_loop(self):
        pass

    def handle_message(self, message: DAQJobMessage) -> bool:
        if not self.can_handle_message(message):
            raise Exception(
                f"Invalid message type '{type(message)}' for DAQJob '{type(self).__name__}'"
            )
        return super().handle_message(message)

    @classmethod
    def can_handle_message(cls, message: DAQJobMessage) -> bool:
        """
        Determines if the given message can be stored based on its configuration.
        Args:
            message (DAQJobMessage): The message to be checked.
        Returns:
            bool: True if the message can be stored, False otherwise.
        """
        if isinstance(message, DAQJobMessageStop):
            return True
        if not isinstance(message, DAQJobMessageStore):
            return False
        is_message_allowed = False
        for allowed_config_type in cls.allowed_store_config_types:
            if message.store_config.has_store_config(allowed_config_type):
                is_message_allowed = True
        return is_message_allowed
