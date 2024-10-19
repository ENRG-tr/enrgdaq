import logging
import threading
import uuid
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Any

from daq.models import DAQJobConfig, DAQJobMessage, DAQJobMessageStop, DAQJobStopError

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

    _logger: logging.Logger

    def __init__(self, config: Any):
        global daq_job_instance_id, daq_job_instance_id_lock

        with daq_job_instance_id_lock:
            self.instance_id = daq_job_instance_id
            daq_job_instance_id += 1
        self._logger = logging.getLogger(f"{type(self).__name__}({self.instance_id})")

        self.config = config
        self.message_in = Queue()
        self.message_out = Queue()

        self._should_stop = False
        self.unique_id = str(uuid.uuid4())

    def consume(self):
        # consume messages from the queue
        while True:
            try:
                message = self.message_in.get_nowait()
                if not self.handle_message(message):
                    self.message_in.put_nowait(message)
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

    def get_info(self) -> "DAQJobInfo":
        return DAQJobInfo(
            daq_job_type=self.config.daq_job_type
            if isinstance(self.config, DAQJobConfig)
            else self.config["daq_job_type"],
            daq_job_class_name=type(self).__name__,
            unique_id=self.unique_id,
            instance_id=self.instance_id,
        )

    def __del__(self):
        self._logger.info("DAQ job is being deleted")


@dataclass
class DAQJobThread:
    daq_job: DAQJob
    thread: threading.Thread


@dataclass
class DAQJobInfo:
    daq_job_type: str
    daq_job_class_name: str  # has type(self).__name__
    unique_id: str
    instance_id: int
