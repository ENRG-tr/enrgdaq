import logging
import multiprocessing
import pickle
import queue
import re
import sys
import threading
import uuid
from datetime import datetime, timedelta
from logging.handlers import QueueHandler
from multiprocessing import Process
from multiprocessing.shared_memory import SharedMemory
from queue import Queue as ThreadQueue
from typing import Any, Optional

import msgspec
import zmq

from enrgdaq.daq.models import (
    DAQJobConfig,
    DAQJobInfo,
    DAQJobMessage,
    DAQJobMessageJobStarted,
    DAQJobMessageSHM,
    DAQJobMessageStatsReport,
    DAQJobMessageStop,
    DAQJobStopError,
    InternalDAQJobMessage,
    LogVerbosity,
    SHMHandle,
)
from enrgdaq.daq.store.models import (
    DAQJobMessageStore,
    DAQJobMessageStorePyArrow,
    DAQJobMessageStoreSHM,
)
from enrgdaq.daq.topics import Topic
from enrgdaq.message_broker import send_message
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

    allowed_message_in_types = []

    topics_to_subscribe: list[str] = []
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
    _consume_thread: threading.Thread | None = None

    def __init__(
        self,
        config: DAQJobConfig,
        supervisor_info: SupervisorInfo | None = None,
        instance_id: int | None = None,
        message_in: Any = None,
        message_out: Any = None,
        raw_config: str | None = None,
        zmq_xpub_url: str | None = None,
        zmq_xsub_url: str | None = None,
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

        self._supervisor_info = supervisor_info

        self.info = self._create_info(raw_config)
        self._logger.debug(f"DAQ job {self.info.unique_id} created")

        self._watchdog = Watchdog(
            timeout_seconds=self.watchdog_timeout_seconds,
            force_exit=self.watchdog_force_exit,
            logger=self._logger,
        )

        self._latency_samples: list[float] = []
        self._processed_count = 0
        self._processed_bytes = 0
        self._sent_count = 0
        self._sent_bytes = 0
        self._last_stats_report_time = datetime.now()

        self.topics_to_subscribe.extend(
            [
                Topic.supervisor_broadcast(self.supervisor_id),
                Topic.daq_job_direct(type(self).__name__, self.unique_id),
            ]
        )
        self.info.subscribed_topics = self.topics_to_subscribe

        self._zmq_xpub_url = zmq_xpub_url
        self._zmq_xsub_url = zmq_xsub_url

        self._consume_thread = threading.Thread(
            target=self._consume_thread_func, daemon=True
        )
        self._publish_thread = threading.Thread(
            target=self._publish_thread_func, daemon=True
        )
        self._publish_buffer = queue.Queue()
        self._consume_thread.start()
        self._publish_thread.start()

    def get_job_started_message(self):
        return self._prepare_message(DAQJobMessageJobStarted())

    def _consume_thread_func(self):
        assert self._zmq_xpub_url is not None

        # Connect to zmq xpub
        self.zmq_context = zmq.Context()
        zmq_xpub = self.zmq_context.socket(zmq.SUB)
        zmq_xpub.setsockopt_string(zmq.IDENTITY, self.unique_id)
        zmq_xpub.connect(self._zmq_xpub_url)
        # Subscribe to topics
        for topic in self.topics_to_subscribe:
            zmq_xpub.subscribe(topic)

        self._logger.debug(
            f"Subscribed to topics: {', '.join(self.topics_to_subscribe)}"
        )

        # Start receiving messages
        while not self._has_been_freed:
            try:
                parts = zmq_xpub.recv_multipart()
                topic = parts[0]
                header = parts[1]
                buffers = parts[2:]

                message_len = len(header) + sum(len(b) for b in buffers)
                recv_message = pickle.loads(header, buffers=buffers)
                if isinstance(recv_message, DAQJobMessageSHM) or isinstance(
                    recv_message, DAQJobMessageStoreSHM
                ):
                    message_len += recv_message.shm.shm_size
                elif isinstance(recv_message, DAQJobMessageStorePyArrow):
                    if recv_message.handle is not None:
                        message_len += recv_message.handle.data_size

                recv_message = self._unwrap_message(recv_message)
                self._logger.debug(
                    f"Received message of size {message_len} bytes '{type(recv_message).__name__}' on topic '{topic.decode()}'"
                )
                recv_message.is_remote = True
                self.handle_message(recv_message)
                self._processed_bytes += message_len
                self.report_stats()
            except zmq.ContextTerminated:
                break
            except Exception as e:
                self._logger.error(
                    f"Error while unpacking message sent in {topic}: {e}",
                    exc_info=True,
                )

    def _publish_thread_func(self):
        assert self._zmq_xsub_url is not None
        # Connect to zmq xsub
        self.zmq_context = zmq.Context()
        zmq_xsub = self.zmq_context.socket(zmq.PUB)
        zmq_xsub.setsockopt_string(zmq.IDENTITY, self.unique_id)
        zmq_xsub.connect(self._zmq_xsub_url)

        buffer = []
        while not self._has_been_freed:
            message: DAQJobMessage = self._publish_buffer.get()
            if message is None:
                break
            send_message(zmq_xsub, message, buffer)
            self.report_stats()

    def _unwrap_message(self, message: DAQJobMessage) -> DAQJobMessage:
        if isinstance(message, DAQJobMessageSHM) or isinstance(
            message, DAQJobMessageStoreSHM
        ):
            res = message.shm.load()
            message.shm.cleanup()
            return res
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
            self.free()
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
            subscribed_topics=self.topics_to_subscribe,
        )

    def _prepare_message(
        self, message: DAQJobMessage, use_shm=False, modify_message_metadata=True
    ):
        """
        Prepares a message for sending.

        Args:
            message (DAQJobMessage): The message to put in the queue.
        """

        if modify_message_metadata:
            message.daq_job_info = self.info

        omit_debug_message = not isinstance(
            message, DAQJobMessageStatsReport
        ) and not isinstance(message, InternalDAQJobMessage)
        if self.config.verbosity == LogVerbosity.DEBUG and omit_debug_message:
            self._logger.debug(f"Message out: {_format_message_for_log(message)}")

        if use_shm and self.config.use_shm_when_possible and sys.platform != "win32":
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
                    message.topics = original_message.topics
                else:
                    # Fall back to regular pickle-based SHM
                    pass
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
                message.topics = original_message.topics

        return message

    def _put_message_out(
        self, message: DAQJobMessage, use_shm=False, modify_message_metadata=True
    ):
        """
        Sends the message to its described topics.

        Args:
            message (DAQJobMessage): The message to put in the queue.
        """
        message = self._prepare_message(message, use_shm, modify_message_metadata)
        self._publish_buffer.put(message)
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

        self._last_stats_report_time = datetime.now()
        report = DAQJobMessageStatsReport(
            processed_count=self._processed_count,
            processed_bytes=self._processed_bytes,
            sent_count=self._sent_count,
            sent_bytes=self._sent_bytes,
            latency=self.get_latency_stats(),
        )
        self._put_message_out(report)
        self._latency_samples = []  # RESET after report to get interval stats

    def __del__(self):
        self._logger.info("DAQ job is being deleted")
        self._has_been_freed = True

    @property
    def supervisor_id(self):
        if self.info.supervisor_info is None:
            return "unknown"
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
    process: Process | None
    start_time: datetime = msgspec.field(default_factory=datetime.now)
    instance_id: int
    daq_job_info: DAQJobInfo | None = None
    raw_config: str | None = None
    log_queue: Any | None = None
    restart_on_crash: bool = True

    zmq_xpub_url: str | None = None
    zmq_xsub_url: str | None = None

    job_started_queue: multiprocessing.Queue = msgspec.field(
        default_factory=multiprocessing.Queue
    )

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
            raw_config=self.raw_config,
            zmq_xpub_url=self.zmq_xpub_url,
            zmq_xsub_url=self.zmq_xsub_url,
        )
        self.job_started_queue.put(instance.get_job_started_message())

        try:
            instance.start()
        except Exception as e:
            logging.error(
                f"Error on {self.daq_job_cls.__name__}.start(): {e}", exc_info=True
            )
            raise e
