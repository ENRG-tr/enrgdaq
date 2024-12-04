import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from queue import Empty, Queue
from typing import Any, Optional

from enrgdaq.daq.models import (
    DAQJobConfig,
    DAQJobInfo,
    DAQJobMessage,
    DAQJobMessageStop,
    DAQJobStopError,
)
from enrgdaq.daq.store.models import DAQJobMessageStore
from enrgdaq.models import SupervisorConfig

daq_job_instance_id = 0
daq_job_instance_id_lock = threading.Lock()


class DAQJob:
    """
    DAQJob is a base class for data acquisition jobs. It handles the configuration,
    message queues, and provides methods for consuming and handling messages.
    Attributes:
        allowed_message_in_types (list[type[DAQJobMessage]]): List of allowed message types for input.
        config_type (Any): Type of the configuration.
        config (Any): Configuration object.
        message_in (Queue[DAQJobMessage]): Queue for incoming messages.
        message_out (Queue[DAQJobMessage]): Queue for outgoing messages.
        instance_id (int): Unique instance identifier.
        unique_id (str): Unique identifier for the job.
        restart_offset (timedelta): Offset for restarting the job.
        info (DAQJobInfo): Information about the job.
        _has_been_freed (bool): Flag indicating if the job has been freed.
        _logger (logging.Logger): Logger instance for the job.
    """

    allowed_message_in_types: list[type[DAQJobMessage]] = []
    config_type: Any
    config: Any
    message_in: Queue[DAQJobMessage]
    message_out: Queue[DAQJobMessage]
    instance_id: int
    unique_id: str
    restart_offset: timedelta
    info: "DAQJobInfo"
    _has_been_freed: bool
    _logger: logging.Logger

    def __init__(
        self, config: Any, supervisor_config: Optional[SupervisorConfig] = None
    ):
        global daq_job_instance_id, daq_job_instance_id_lock

        with daq_job_instance_id_lock:
            self.instance_id = daq_job_instance_id
            daq_job_instance_id += 1
        self._logger = logging.getLogger(f"{type(self).__name__}({self.instance_id})")
        if isinstance(config, DAQJobConfig):
            self._logger.setLevel(config.verbosity.to_logging_level())

        self.config = config
        self.message_in = Queue()
        self.message_out = Queue()

        self._has_been_freed = False
        self.unique_id = str(uuid.uuid4())

        if supervisor_config is not None:
            self._supervisor_config = supervisor_config
        else:
            self._supervisor_config = None
        self.info = self._create_info()

    def consume(self, nowait=True):
        """
        Consumes messages from the message_in queue.
        If nowait is True, it will consume the message immediately.
        Otherwise, it will wait until a message is available.
        """

        def _process_message(message):
            if not self.handle_message(message):
                self.message_in.put(message)

        # Return immediately after consuming the message
        if not nowait:
            _process_message(self.message_in.get())
            return

        while True:
            try:
                _process_message(self.message_in.get_nowait())
            except Empty:
                break

    def handle_message(self, message: "DAQJobMessage") -> bool:
        """
        Handles a message received from the message queue.

        Args:
            message (DAQJobMessage): The message to handle.

        Returns:
            bool: True if the message was handled, False otherwise.

        Raises:
            DAQJobStopError: If the message is a DAQJobMessageStop.
            Exception: If the message is not accepted by the job.
        """

        if isinstance(message, DAQJobMessageStop):
            raise DAQJobStopError(message.reason)
        # check if the message is accepted
        is_message_type_accepted = False
        for accepted_message_type in self.allowed_message_in_types:
            if isinstance(message, accepted_message_type):
                is_message_type_accepted = True
        if not is_message_type_accepted:
            raise Exception(
                f"Message type '{type(message)}' is not accepted by '{type(self).__name__}'"
            )
        # Drop remote messages silently if configured to do so
        if self.config.remote_config.drop_remote_messages:
            if message.daq_job_info and message.daq_job_info.supervisor_config:
                remote_supervisor_id = (
                    message.daq_job_info.supervisor_config.supervisor_id
                )
            else:
                remote_supervisor_id = "unknown"
            self._logger.debug(
                f"Dropping remote message '{type(message)}' from '{remote_supervisor_id}' because drop_remote_messages is True"
            )
            return True
        return True

    def start(self):
        raise NotImplementedError

    def _create_info(self) -> "DAQJobInfo":
        """
        Creates a DAQJobInfo object for the job.

        Returns:
            DAQJobInfo: The created DAQJobInfo object.
        """

        return DAQJobInfo(
            daq_job_type=self.config.daq_job_type
            if isinstance(self.config, DAQJobConfig)
            else self.config["daq_job_type"],
            daq_job_class_name=type(self).__name__,
            unique_id=self.unique_id,
            instance_id=self.instance_id,
            supervisor_config=getattr(self, "_supervisor_config", None),
        )

    def _put_message_out(self, message: DAQJobMessage):
        """
        Puts a message in the message_out queue.

        Should be called by DAQJob itself.

        Args:
            message (DAQJobMessage): The message to put in the queue.
        """

        message.daq_job_info = self.info
        message.remote_config = self.config.remote_config

        # Get the remote config from the store config if it exists
        if isinstance(message, DAQJobMessageStore):
            store_remote_config = message.get_remote_config()
            if store_remote_config is not None:
                message.remote_config = store_remote_config

        self.message_out.put(message)

    def __del__(self):
        self._logger.info("DAQ job is being deleted")
        self._has_been_freed = True

    def free(self):
        if self._has_been_freed:
            return
        self._has_been_freed = True
        self.__del__()


@dataclass
class DAQJobThread:
    daq_job: DAQJob
    thread: threading.Thread
    start_time: datetime = field(default_factory=datetime.now)
