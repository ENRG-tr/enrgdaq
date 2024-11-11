import logging
import threading
import uuid
from dataclasses import dataclass
from datetime import timedelta
from queue import Empty, Queue
from typing import Any, Optional

from daq.models import (
    DAQJobConfig,
    DAQJobInfo,
    DAQJobMessage,
    DAQJobMessageStop,
    DAQJobStopError,
)
from daq.store.models import DAQJobMessageStore
from models import SupervisorConfig

daq_job_instance_id = 0
daq_job_instance_id_lock = threading.Lock()


class DAQJob:
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
        return True

    def start(self):
        raise NotImplementedError

    def _create_info(self) -> "DAQJobInfo":
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
