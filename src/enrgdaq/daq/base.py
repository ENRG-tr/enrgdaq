import logging
import pickle
import re
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta
from logging.handlers import QueueHandler
from multiprocessing import Process
from multiprocessing.shared_memory import SharedMemory
from queue import Empty
from queue import Queue as ThreadQueue
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
    DAQJobMessageStatsReport,
)
from enrgdaq.daq.store.models import (
    DAQJobMessageStore,
    DAQJobMessageStorePyArrow,
    DAQJobMessageStoreSHM,
)
from enrgdaq.models import SupervisorInfo
from enrgdaq.utils.queue import ZMQQueue
from enrgdaq.utils.test import is_unit_testing
from enrgdaq.utils.watchdog import Watchdog

DAQ_JOB_STATS_REPORT_INTERVAL_SECONDS = 1.0


def _format_message_for_log(
    message: "DAQJobMessage", max_bytes_preview: int = 32
) -> str:
    """Format a message for debug logging, truncating bytes data."""
    # Get a string representation and truncate any bytes data
    msg_str = str(message)

    def truncate_bytes(match: re.Match) -> str:
        prefix = match.group(1)  # 'data=' or empty
        bytes_content = match.group(2)  # The actual bytes content
        if len(bytes_content) > max_bytes_preview:
            return f"{prefix}b'<{len(bytes_content)} bytes>'"
        return match.group(0)

    # Match bytes literals: optional prefix like 'data=' followed by b'...'
    msg_str = re.sub(r"(data=)?b'([^']*)'", truncate_bytes, msg_str)
    msg_str = re.sub(r'(data=)?b"([^"]*)"', truncate_bytes, msg_str)

    return msg_str


def _create_queue(maxsize: int = 0) -> Any:
    """Create a queue appropriate for the current context.

    In unit testing, uses queue.Queue to avoid pickle issues with MagicMock.
    In production, uses ZMQQueue for fast cross-process communication.
    """
    if is_unit_testing():
        return ThreadQueue(maxsize=maxsize)
    return ZMQQueue(maxsize=maxsize)


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
    message_in: Any
    message_out: Any
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
    _consume_thread: threading.Thread | None = None 

    def __init__(
        self,
        config: DAQJobConfig,
        supervisor_info: SupervisorInfo | None = None,
        instance_id: int | None = None,
        message_in: Any = None,
        message_out: Any = None,
        raw_config: str | None = None,
    ):
        self.instance_id = instance_id or 0
        self._logger = logging.getLogger(f"{type(self).__name__}({self.instance_id})")

        # TODO: In some tests config is a dict, so we need to check for this
        if isinstance(config, DAQJobConfig):  # pyright: ignore[reportUnnecessaryIsInstance]
            self._logger.setLevel(config.verbosity.to_logging_level())

        self.config = config
        self.message_in = message_in or _create_queue()
        self.message_out = message_out or _create_queue()

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

        self._latency_samples: list[float] = []
        self._processed_count = 0
        self._sent_count = 0
        self._last_stats_report_time = datetime.now()

        self._put_message_out(DAQJobMessageJobStarted())

        # Start consume thread if not a store job
        from enrgdaq.daq.store.base import DAQJobStore
        # TODO: FIX THISSSSS!! NOO!
        if not isinstance(self, DAQJobStore):
            self._consume_thread = threading.Thread(target=self._consume_thread_func, daemon=True)
            self._consume_thread.start()

    def _consume_thread_func(self):
        while True:
            if not self.consume_all():
                time.sleep(0.001)

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
            return self.handle_message(msg)

        while True:
            try:
                msg = self.message_in.get_nowait()
                msg = self._unwrap_message(msg)
                return self.handle_message(msg)
            except (Empty, FileNotFoundError):
                break
        self.report_stats()

        return False


    def consume_all(self):
        processed_any = False
        while True:
            try:
                msg = self.message_in.get_nowait()
                msg = self._unwrap_message(msg)
                if self.handle_message(msg):
                    processed_any = True
            except (Empty, FileNotFoundError):
                break

        self.report_stats()
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

        self._processed_count += 1
        from enrgdaq.daq.store.models import DAQJobMessageStore

        if message.timestamp and isinstance(message, DAQJobMessageStore):
            latency = (datetime.now() - message.timestamp).total_seconds() * 1000.0
            self._latency_samples.append(latency)
            # Keep only last 1000 samples to prevent memory leak
            if len(self._latency_samples) > 1000:
                self._latency_samples.pop(0)

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

    def _put_message_out(
        self, message: DAQJobMessage, use_shm=False, modify_message_metadata=True
    ):
        """
        Puts a message in the message_out queue.

        Should be called by DAQJob itself.

        Args:
            message (DAQJobMessage): The message to put in the queue.
        """

        if modify_message_metadata:
            message.daq_job_info = self.info
            message.remote_config = self.config.remote_config

            # Get the remote config from the store config if it exists
            if isinstance(message, DAQJobMessageStore):
                store_remote_config = message.get_remote_config()
                if store_remote_config is not None:
                    message.remote_config = store_remote_config

        omit_debug_message = isinstance(message, DAQJobMessageStatsReport)
        if self.config.verbosity == LogVerbosity.DEBUG and not omit_debug_message:
            self._logger.debug(f"Message out: {_format_message_for_log(message)}")

        if use_shm and sys.platform != "win32":
            original_message = message

            # Use zero-copy ring buffer for PyArrow messages
            if (
                isinstance(message, DAQJobMessageStorePyArrow)
                and message.table is not None
            ):
                from enrgdaq.utils.arrow_ipc import try_zero_copy_pyarrow

                handle, success = try_zero_copy_pyarrow(
                    message.table,
                    message.store_config,
                    message.tag,
                )
                if success and handle is not None:
                    message = DAQJobMessageStorePyArrow(
                        store_config=message.store_config,
                        tag=message.tag,
                        table=None,
                        handle=handle,
                    )
                    message.daq_job_info = self.info
                    message.remote_config = self.config.remote_config
                    message.route_keys = original_message.route_keys
                else:
                    # Fall back to regular pickle-based SHM
                    pass  # Continue to standard SHM handling below
            else:
                # For non-PyArrow messages, use the existing pickle-based approach
                pass

            # Standard SHM path for non-PyArrow or fallback
            if not (
                isinstance(message, DAQJobMessageStorePyArrow)
                and message.handle is not None
            ):
                # Pickle message
                message_bytes = pickle.dumps(message)
                # Create shared memory object
                shm = SharedMemory(create=True, size=len(message_bytes))
                assert shm.buf is not None, "Shared memory buffer is None"
                # Write message to shared memory
                shm.buf[: len(message_bytes)] = message_bytes
                shm.close()
                shm_handle = SHMHandle(shm_name=shm.name, shm_size=shm.size)
                if isinstance(original_message, DAQJobMessageStore):
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
                message.route_keys = original_message.route_keys

        all_routes_satisfied = True
        if self._routes is not None:
            for route_key in message.route_keys:
                if route_key in self._routes:
                    for queue in self._routes[route_key]:
                        queue.put(message)
                        if not omit_debug_message:
                            self._logger.debug(
                                f"Direct routed {type(message).__name__} to {route_key}"
                            )
                        break
                else:
                    pass
                    # This route_key is not available locally
                    # all_routes_satisfied = False
                    # self._logger.debug(f"Route key {route_key} not found locally")
        else:
            all_routes_satisfied = False
            if not omit_debug_message:
                self._logger.debug(f"No routes available, sending to supervisor ({self._routes} routes)")

        # Send to supervisor if any route was not satisfied locally
        if not all_routes_satisfied:
            self.message_out.put(message)
        self._sent_count += 1

    def get_latency_stats(self) -> Any:
        from enrgdaq.daq.models import DAQJobLatencyStats

        if not self._latency_samples:
            return DAQJobLatencyStats()

        samples = sorted(self._latency_samples)
        count = len(samples)
        return DAQJobLatencyStats(
            count=self._processed_count,
            min_ms=samples[0],
            max_ms=samples[-1],
            avg_ms=sum(samples) / count,
            p95_ms=samples[int(count * 0.95)],
            p99_ms=samples[int(count * 0.99)],
        )

    def report_stats(self, force: bool = False):
        if (
            not force
            and (datetime.now() - self._last_stats_report_time).total_seconds()
            < DAQ_JOB_STATS_REPORT_INTERVAL_SECONDS
        ):
            return

        from enrgdaq.daq.models import DAQJobMessageStatsReport

        self._last_stats_report_time = datetime.now()
        report = DAQJobMessageStatsReport(
            processed_count=self._processed_count,
            sent_count=self._sent_count,
            latency=self.get_latency_stats(),
        )
        self._put_message_out(report)
        self._latency_samples = []  # RESET after report to get interval stats

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
    message_in: Any
    message_out: Any
    process: Process | None
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
