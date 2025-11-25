import time

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import DAQJobMessage
from enrgdaq.daq.store.models import DAQJobMessageStore

STORE_LOOP_INTERVAL_SECONDS = 0.01


class DAQJobStore(DAQJob):
    """
    DAQJobStore is an abstract base class for data acquisition job stores. It extends the DAQJob class
    and provides additional functionality for handling and storing messages.
    Attributes:
        allowed_store_config_types (list): A list of allowed store configuration types.
    """

    allowed_store_config_types: list

    def start(self):
        """
        Starts the continuous loop for consuming and storing data.
        This method runs an infinite loop that repeatedly calls the `consume`
        and `store_loop` methods.
        """

        while True:
            self.consume()
            self.store_loop()
            time.sleep(STORE_LOOP_INTERVAL_SECONDS)

    def store_loop(self):
        raise NotImplementedError

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

        if not isinstance(message, DAQJobMessageStore):
            return False
        is_message_allowed = False
        for allowed_config_type in cls.allowed_store_config_types:
            if message.store_config.has_store_config(allowed_config_type):
                is_message_allowed = True
        return is_message_allowed
