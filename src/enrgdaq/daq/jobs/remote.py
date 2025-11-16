import pickle
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from queue import Empty
from typing import Optional

import msgspec
import zmq
from msgspec import Struct, field

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import (
    DEFAULT_REMOTE_TOPIC,
    DAQJobConfig,
    DAQJobMessage,
)
from enrgdaq.utils.subclasses import all_subclasses
from enrgdaq.utils.time import sleep_for

DAQ_JOB_REMOTE_MAX_REMOTE_MESSAGE_ID_COUNT = 10000
DAQ_JOB_REMOTE_STATS_SEND_INTERVAL_SECONDS = 1
DAQ_JOB_REMOTE_QUEUE_ACTION_TIMEOUT = 1


class SupervisorRemoteStats(Struct):
    """Statistics for a remote supervisor."""

    message_in_count: int = 0
    message_in_bytes: int = 0

    message_out_count: int = 0
    message_out_bytes: int = 0

    last_active: datetime = field(default_factory=datetime.now)

    def update_message_in_stats(self, message_in_bytes: int):
        self.message_in_count += 1
        self.message_in_bytes += message_in_bytes
        self.last_active = datetime.now()

    def update_message_out_stats(self, message_out_bytes: int):
        self.message_out_count += 1
        self.message_out_bytes += message_out_bytes
        self.last_active = datetime.now()


class DAQJobMessageStatsRemote(DAQJobMessage):
    """Message class containing remote statistics."""

    stats: "DAQJobRemoteStatsDict"


DAQJobRemoteStatsDict = dict[str, SupervisorRemoteStats]


class DAQJobRemoteConfig(DAQJobConfig):
    """
    Configuration for DAQJobRemote.

    Attributes:
        zmq_local_url (str): Local ZMQ URL.
        zmq_remote_urls (list[str]): List of remote ZMQ URLs.
        topics (list[str]): List of topics to subscribe to.
    """

    zmq_proxy_sub_urls: list[str]
    topics: list[str] = []
    zmq_proxy_pub_url: Optional[str] = None

    zmq_router_url: Optional[str] = None
    zmq_dealer_url: Optional[str] = None


class DAQJobRemote(DAQJob):
    """
    DAQJobRemote is a DAQJob that connects two separate ENRGDAQ instances.
    It sends to and receives from a remote ENRGDAQ, such that:

    - message_in -> remote message_out
    - remote message_in -> message_out

    Attributes:
        allowed_message_in_types (list): List of allowed message types.
        config_type (type): Configuration type for the job.
        config (DAQJobRemoteConfig): Configuration instance.
        restart_offset (timedelta): Restart offset time.
        _message_class_cache (dict): Cache for message classes.
        _remote_message_ids (set): Set of remote message IDs.
        _receive_thread (threading.Thread): Thread for receiving messages.
    """

    allowed_message_in_types = [DAQJobMessage]  # accept all message types
    config_type = DAQJobRemoteConfig
    config: DAQJobRemoteConfig
    restart_offset = timedelta(seconds=5)

    _message_class_cache: dict[str, type[DAQJobMessage]]
    _remote_message_ids: set[str]
    _receive_thread: threading.Thread
    _remote_stats: DAQJobRemoteStatsDict
    _remote_stats_last_sent_at: float

    def __init__(self, config: DAQJobRemoteConfig, **kwargs):
        super().__init__(config, **kwargs)
        if self.config.zmq_proxy_pub_url is not None:
            self._zmq_pub_ctx = zmq.Context()
            self._zmq_pub = self._zmq_pub_ctx.socket(zmq.PUB)
            self._zmq_pub.connect(self.config.zmq_proxy_pub_url)
        else:
            self._zmq_pub_ctx = None
            self._zmq_pub = None
        self._zmq_sub = None

        self._receive_thread = threading.Thread(
            target=self._start_receive_thread,
            args=(self.config.zmq_proxy_sub_urls,),
            daemon=True,
        )
        self._send_remote_stats_thread = threading.Thread(
            target=self._start_send_remote_stats_thread,
            daemon=True,
        )
        self._message_class_cache = {}

        self._message_class_cache = {
            x.__name__: x for x in all_subclasses(DAQJobMessage)
        }
        self._remote_message_ids = set()
        self._remote_stats = defaultdict(lambda: SupervisorRemoteStats())
        self._remote_stats_last_sent_at = datetime.now().timestamp()

    def handle_message(self, message: DAQJobMessage) -> bool:
        if (
            # Ignore if we already received the message
            message.id in self._remote_message_ids
            # Ignore if the message is not allowed by the DAQ Job
            or not super().handle_message(message)
            # Ignore if the message is remote, meaning it was sent by another Supervisor
            or message.is_remote
            # Ignore if we are not connected to the proxy
            or self._zmq_pub is None
        ):
            return True  # Silently ignore

        if message.remote_config.remote_disable:
            return True

        self._send_remote_pub_message(message)
        return True

    def _send_remote_pub_message(self, message: DAQJobMessage):
        if self._zmq_pub is None:
            return

        remote_topic = message.remote_config.remote_topic or DEFAULT_REMOTE_TOPIC
        remote_topic_bytes = remote_topic.encode()
        packed_message = self._pack_message(message)
        self._zmq_pub.send_multipart([remote_topic_bytes, packed_message])

        # Update remote stats
        if self._supervisor_config:
            self._remote_stats[
                self._supervisor_config.supervisor_id
            ].update_message_out_stats(len(packed_message) + len(remote_topic_bytes))

        self._logger.debug(
            f"Sent message '{type(message).__name__}' to topic '{remote_topic}'"
        )
        return True

    def _create_zmq_sub(self, remote_urls: list[str]) -> zmq.Socket:
        """
        Create a ZMQ subscriber socket.

        Args:g
            remote_urls (list[str]): List of remote URLs to connect to.

        Returns:
            zmq.Socket: The created ZMQ subscriber socket.
        """
        self._zmq_sub_ctx = zmq.Context()
        zmq_sub = self._zmq_sub_ctx.socket(zmq.SUB)
        for remote_url in remote_urls:
            self._logger.debug(f"Connecting to {remote_url}")
            zmq_sub.connect(remote_url)
            topics_to_subscribe = [DEFAULT_REMOTE_TOPIC]
            topics_to_subscribe.extend(self.config.topics)
            # Subscribe to the supervisor id if we have it
            if self.info.supervisor_config is not None:
                topics_to_subscribe.append(self.info.supervisor_config.supervisor_id)
            for topic in topics_to_subscribe:
                zmq_sub.subscribe(topic)

            self._logger.info(f"Subscribed to topics: {", ".join(topics_to_subscribe)}")
        return zmq_sub

    def _start_receive_thread(self, remote_urls: list[str]):
        """
        Start the receive thread.

        Args:
            remote_urls (list[str]): List of remote URLs to connect to.
        """
        self._zmq_sub = self._create_zmq_sub(remote_urls)

        while True:
            try:
                topic, message = self._zmq_sub.recv_multipart()
            except zmq.ContextTerminated:
                break
            try:
                recv_message = self._unpack_message(message)
            except Exception as e:
                self._logger.error(
                    f"Error while unpacking message sent in {topic}: {e}", exc_info=True
                )
                continue
            if (
                recv_message.daq_job_info is not None
                and recv_message.daq_job_info.supervisor_config is not None
                and self.info.supervisor_config is not None
                and recv_message.daq_job_info.supervisor_config.supervisor_id
                == self.info.supervisor_config.supervisor_id
            ):
                self._logger.warning(
                    f"Received own message '{type(recv_message).__name__}' on topic '{topic.decode()}', ignoring message. This should NOT happen. Check the config."
                )
                continue
            self._logger.debug(
                f"Received {len(message)} bytes for message '{type(recv_message).__name__}' on topic '{topic.decode()}'"
            )
            recv_message.is_remote = True
            # remote message_in -> message_out
            self.message_out.put(recv_message)

            # Update remote stats
            if self._supervisor_config:
                self._remote_stats[
                    self._supervisor_config.supervisor_id
                ].update_message_in_stats(len(message))
            if (
                recv_message.daq_job_info
                and recv_message.daq_job_info.supervisor_config
            ):
                self._remote_stats[
                    recv_message.daq_job_info.supervisor_config.supervisor_id
                ].update_message_out_stats(len(message))

    def _start_send_remote_stats_thread(self):
        while True:
            self._send_remote_stats_message()
            sleep_for(DAQ_JOB_REMOTE_STATS_SEND_INTERVAL_SECONDS)

    def start(self):
        """
        Start the receive thread and the DAQ job.
        """
        self._receive_thread.start()
        self._send_remote_stats_thread.start()

        while True:
            # message_in -> remote message_out
            try:
                self.consume(nowait=False, timeout=DAQ_JOB_REMOTE_QUEUE_ACTION_TIMEOUT)
            except Empty:
                if not self._receive_thread.is_alive():
                    raise RuntimeError("Receive thread is dead")
                if not self._send_remote_stats_thread.is_alive():
                    raise RuntimeError("Send remote stats thread is dead")

    def _pack_message(self, message: DAQJobMessage, use_pickle: bool = True) -> bytes:
        """
        Pack a message for sending.

        Args:
            message (DAQJobMessage): The message to pack.
            use_pickle (bool): Whether to use pickle for packing, if not, use msgspec.

        Returns:
            bytes: The packed message.
        """
        message_type = type(message).__name__
        if use_pickle:
            return pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)

        return msgspec.msgpack.encode([message_type, message])

    def _unpack_message(self, message: bytes) -> DAQJobMessage:
        """
        Unpack a received message.

        It tries to unpack the message using pickle, and if that fails, it uses msgspec.

        Args:
            message (bytes): The received message.

        Returns:
            DAQJobMessage: The unpacked message.
        """
        # TODO: fix unpack without pickle
        try:
            res = pickle.loads(message)
            if not isinstance(res, DAQJobMessage):
                raise Exception("Message is not DAQJobMessage")
            message_type = type(res).__name__
        except pickle.UnpicklingError:
            message_type, data = msgspec.msgpack.decode(message)
            if message_type not in self._message_class_cache:
                raise Exception(f"Invalid message type: {message_type}")
            message_class = self._message_class_cache[message_type]

            res = msgspec.convert(data, type=message_class)

        if res.id is None:
            raise Exception("Message id is not set")
        self._remote_message_ids.add(res.id)
        if len(self._remote_message_ids) > DAQ_JOB_REMOTE_MAX_REMOTE_MESSAGE_ID_COUNT:
            self._remote_message_ids.pop()
        return res

    def _send_remote_stats_message(self):
        msg = DAQJobMessageStatsRemote(dict(self._remote_stats))
        self._put_message_out(msg)

    def __del__(self):
        """
        Destructor for DAQJobRemote.
        """
        if getattr(self, "_zmq_sub_ctx", None) is not None:
            self._zmq_sub_ctx.destroy()
        if self._zmq_pub_ctx is not None:
            self._zmq_pub_ctx.destroy()

        return super().__del__()
