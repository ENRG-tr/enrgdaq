import logging
import pickle
import sys
import uuid
from datetime import datetime, timedelta
from logging.handlers import QueueHandler
from multiprocessing import Process, Queue
from multiprocessing.context import ForkProcess
from multiprocessing.shared_memory import SharedMemory
from queue import Empty
from typing import Any, Optional

import msgspec

from enrgdaq.daq.models import (
    DAQJobConfig,
    DAQJobInfo,
    DAQJobMessage,
    DAQJobMessageJobStarted,
    DAQJobMessageRoutes,
    DAQJobMessageSHM,
    DAQJobMessageStop,
    DAQJobStopError,
    LogVerbosity,
    RouteMapping,
    SHMHandle,
)
from enrgdaq.daq.store.models import (
    DAQJobMessageStore,
    DAQJobMessageStoreSHM,
)
from enrgdaq.models import SupervisorInfo
from enrgdaq.utils.watchdog import Watchdog


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
    config_type: type[DAQJobConfig]  # pyright: ignore[reportUninitializedInstanceVariable]
    config: DAQJobConfig
    message_in: "Queue[DAQJobMessage]"
    message_out: "Queue[DAQJobMessage]"
    instance_id: int
    unique_id: str
    restart_offset: timedelta  # pyright: ignore[reportUninitializedInstanceVariable]
    info: "DAQJobInfo"
    _has_been_freed: bool
    _logger: logging.Logger
    multiprocessing_method: str = "default"  # Can be 'fork', 'spawn', or 'default'
    watchdog_timeout_seconds: float = 0  # Watchdog timeout in seconds, 0 = disabled
    watchdog_force_exit: bool = (
        False  # If True, force exit on timeout (for blocking calls)
    )
    _watchdog: Watchdog
    _supervisor_info: SupervisorInfo | None
    _routes: RouteMapping | None = None

    def __init__(
        self,
        config: DAQJobConfig,
        supervisor_info: SupervisorInfo | None = None,
        instance_id: int | None = None,
        message_in: "Queue[DAQJobMessage] | None" = None,
        message_out: "Queue[DAQJobMessage] | None" = None,
        raw_config: str | None = None,
    ):
        self.instance_id = instance_id or 0
        self._logger = logging.getLogger(f"{type(self).__name__}({self.instance_id})")

        # TODO: In some tests config is a dict, so we need to check for this
        if isinstance(config, DAQJobConfig):  # pyright: ignore[reportUnnecessaryIsInstance]
            self._logger.setLevel(config.verbosity.to_logging_level())

        self.config = config
        self.message_in = message_in or Queue()
        self.message_out = message_out or Queue()

        self._has_been_freed = False
        self.unique_id = getattr(config, "daq_job_unique_id", None) or str(uuid.uuid4())

        if supervisor_info is not None:
            self._supervisor_info = supervisor_info
        else:
            self._supervisor_info = None
        self.info = self._create_info(raw_config)
        self._logger.debug(f"DAQ job {self.info.unique_id} created")

        self._watchdog = Watchdog(
            timeout_seconds=self.watchdog_timeout_seconds,
            force_exit=self.watchdog_force_exit,
            logger=self._logger,
        )

        self._put_message_out(DAQJobMessageJobStarted())

    def consume(self, nowait: bool = True, timeout: float | None = None):
        """
        Consumes messages from the message_in queue.
        If nowait is True, it will consume the message immediately.
        Otherwise, it will wait until a message is available.
        """

        # Return immediately after consuming the message
        if not nowait:
            msg = self.message_in.get(timeout=timeout)
            msg = self._unwrap_message(msg)
            if not self.handle_message(msg):
                self.message_in.put(msg)
            return

        while True:
            try:
                msg = self.message_in.get_nowait()
                msg = self._unwrap_message(msg)
                if not self.handle_message(msg):
                    self.message_in.put(msg)
            except Empty:
                break

    def consume_all(self):
        processed_any = False
        while True:
            try:
                msg = self.message_in.get_nowait()
                msg = self._unwrap_message(msg)
                if self.handle_message(msg):
                    processed_any = True
            except Empty:
                break
        return processed_any

    def _unwrap_message(self, message: DAQJobMessage) -> DAQJobMessage:
        if isinstance(message, DAQJobMessageSHM) or isinstance(
            message, DAQJobMessageStoreSHM
        ):
            return message.shm.load()
        return message

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

        if isinstance(message, DAQJobMessageRoutes):
            self._routes = message.routes
            return True
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

    def _create_info(self, raw_config: Optional[str] = None) -> "DAQJobInfo":
        """
        Creates a DAQJobInfo object for the job.

        Returns:
            DAQJobInfo: The created DAQJobInfo object.
        """

        return DAQJobInfo(
            daq_job_type=self.config.daq_job_type
            if isinstance(self.config, DAQJobConfig)
            else self.config["daq_job_type"],
            unique_id=self.unique_id,
            instance_id=self.instance_id,
            supervisor_info=getattr(self, "_supervisor_info", None),
            config=raw_config or "# No config",
        )

    def _put_message_out(self, message: DAQJobMessage, use_shm=False):
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
            self._logger.debug(f"Message out: {message}")

        if use_shm and sys.platform != "win32":
            original_message = message
            # Pickle message
            message_bytes = pickle.dumps(message)
            # Create shared memory object
            shm = SharedMemory(create=True, size=len(message_bytes))
            assert shm.buf is not None, "Shared memory buffer is None"
            # Write message to shared memory
            shm.buf[: len(message_bytes)] = message_bytes
            shm.close()
            shm_handle = SHMHandle(shm_name=shm.name, shm_size=shm.size)
            if isinstance(message, DAQJobMessageStore):
                message = DAQJobMessageStoreSHM(
                    store_config=getattr(original_message, "store_config"),
                    tag=getattr(original_message, "tag", None),
                    shm=shm_handle,
                )
            else:
                message = DAQJobMessageSHM(
                    shm=shm_handle,
                )
            message.daq_job_info = self.info
            message.remote_config = self.config.remote_config

        if self._routes is not None:
            for route_key, queue_list in self._routes.items():
                if route_key in message.route_keys:
                    self._logger.info("Routing through " + route_key)
                    for queue in queue_list:
                        queue.put(message)

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
    process: Process | ForkProcess | None
    start_time: datetime = msgspec.field(default_factory=datetime.now)
    instance_id: int
    daq_job_info: DAQJobInfo | None = None
    raw_config: str | None = None
    log_queue: Any | None = None
    restart_on_crash: bool = True

    def start(self):
        if self.log_queue:
            root_logger = logging.getLogger()
            root_logger.handlers.clear()
            root_logger.addHandler(QueueHandler(self.log_queue))
            root_logger.setLevel(logging.DEBUG)

        instance = self.daq_job_cls(
            self.config,
            supervisor_info=self.supervisor_info,
            instance_id=self.instance_id,
            message_in=self.message_in,
            message_out=self.message_out,
            raw_config=self.raw_config,
        )
        try:
            instance.start()
        except Exception as e:
            logging.error(
                f"Error on {self.daq_job_cls.__name__}.start(): {e}", exc_info=True
            )
            raise e
