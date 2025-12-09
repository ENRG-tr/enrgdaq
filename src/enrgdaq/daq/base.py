import logging
import os
import uuid
from datetime import datetime, timedelta
from multiprocessing import Process, Queue
from queue import Empty
from typing import Any, Optional

import coloredlogs
import msgspec

from enrgdaq.daq.models import (
    DAQJobConfig,
    DAQJobInfo,
    DAQJobMessage,
    DAQJobMessageStop,
    DAQJobStopError,
    LogVerbosity,
)
from enrgdaq.daq.store.models import DAQJobMessageStore
from enrgdaq.models import SupervisorInfo


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
        multiprocessing_method (str): The multiprocessing method to use ('fork' or 'spawn').
    """

    allowed_message_in_types: list[type[DAQJobMessage]] = []
    config_type: Any
    config: Any
    message_in: "Queue[DAQJobMessage]"
    message_out: "Queue[DAQJobMessage]"
    instance_id: int
    unique_id: str
    restart_offset: timedelta
    info: "DAQJobInfo"
    _has_been_freed: bool
    _logger: logging.Logger
    multiprocessing_method: str = "default"  # Can be 'fork', 'spawn', or 'default'

    def __init__(
        self,
        config: Any,
        supervisor_info: Optional[SupervisorInfo] = None,
        instance_id: Optional[int] = None,
        message_in: Optional["Queue[DAQJobMessage]"] = None,
        message_out: Optional["Queue[DAQJobMessage]"] = None,
    ):
        if os.environ.get("ENRGDAQ_IS_UNIT_TESTING") != "True":
            coloredlogs.install(
                level=logging.DEBUG,
                datefmt="%Y-%m-%d %H:%M:%S",
                fmt="%(asctime)s.%(msecs)03d %(hostname)s %(name)s %(levelname)s %(message)s",
            )
        global daq_job_instance_id, daq_job_instance_id_lock

        self.instance_id = instance_id or 0
        self._logger = logging.getLogger(f"{type(self).__name__}({self.instance_id})")
        if isinstance(config, DAQJobConfig):
            self._logger.setLevel(config.verbosity.to_logging_level())

        self.config = config
        self.message_in = message_in or Queue()
        self.message_out = message_out or Queue()

        self._has_been_freed = False
        self.unique_id = str(uuid.uuid4())

        if supervisor_info is not None:
            self._supervisor_info = supervisor_info
        else:
            self._supervisor_info = None
        self.info = self._create_info()
        self._logger.debug(f"DAQ job {self.info.unique_id} created")

    def consume(self, nowait=True, timeout=None):
        """
        Consumes messages from the message_in queue.
        If nowait is True, it will consume the message immediately.
        Otherwise, it will wait until a message is available.
        """

        # Return immediately after consuming the message
        if not nowait:
            msg = self.message_in.get(timeout=timeout)
            if not self.handle_message(msg):
                self.message_in.put(msg)
            return

        while True:
            try:
                msg = self.message_in.get_nowait()
                if not self.handle_message(msg):
                    self.message_in.put(msg)
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
            if message.daq_job_info and message.daq_job_info.supervisor_info:
                remote_supervisor_id = (
                    message.daq_job_info.supervisor_info.supervisor_id
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
            supervisor_info=getattr(self, "_supervisor_info", None),
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

        if self.config.verbosity == LogVerbosity.DEBUG:
            try:
                msg_json = msgspec.json.encode(message)
            except Exception:
                msg_json = str(message)
            if self.config.verbosity == LogVerbosity.DEBUG:
                if len(msg_json) > 1024:
                    self._logger.debug(f"Message out with {message.remote_config}")
                else:
                    self._logger.debug(f"Message out: {msg_json}")
        self.message_out.put(message)

    def __del__(self):
        self._logger.info("DAQ job is being deleted")
        self._has_been_freed = True

    @property
    def supervisor_id(self):
        assert self.info.supervisor_info is not None
        return self.info.supervisor_info.supervisor_id

    def free(self):
        if self._has_been_freed:
            return
        self._has_been_freed = True
        self.__del__()


class DAQJobProcess(msgspec.Struct, kw_only=True):
    daq_job_cls: type[DAQJob]
    supervisor_info: SupervisorInfo
    config: DAQJobConfig
    message_in: "Queue[DAQJobMessage]"
    message_out: "Queue[DAQJobMessage]"
    process: Optional[Process]
    start_time: datetime = msgspec.field(default_factory=datetime.now)
    instance_id: int
    daq_job_info: Optional[DAQJobInfo] = None
    _daq_job_info_queue: "Queue[DAQJobInfo]" = msgspec.field(default_factory=Queue)

    def start(self):
        instance = self.daq_job_cls(
            self.config,
            supervisor_info=self.supervisor_info,
            instance_id=self.instance_id,
            message_in=self.message_in,
            message_out=self.message_out,
        )
        self._daq_job_info_queue.put(instance.info)
        try:
            instance.start()
        except Exception as e:
            logging.error(
                f"Error on {self.daq_job_cls.__name__}.start(): {e}", exc_info=True
            )
            raise e
