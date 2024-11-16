import pickle
import threading
import time
from datetime import timedelta
from typing import Optional

import msgspec
import zmq

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.jobs.handle_stats import DAQJobMessageStats
from enrgdaq.daq.models import (
    DEFAULT_REMOTE_TOPIC,
    DAQJobConfig,
    DAQJobMessage,
)

DAQ_JOB_REMOTE_MAX_REMOTE_MESSAGE_ID_COUNT = 10000


class DAQJobRemoteConfig(DAQJobConfig):
    """
    Configuration for DAQJobRemote.

    Attributes:
        zmq_local_url (str): Local ZMQ URL.
        zmq_remote_urls (list[str]): List of remote ZMQ URLs.
        topics (list[str]): List of topics to subscribe to.
    """

    zmq_local_url: str
    zmq_remote_urls: list[str]
    topics: list[str] = []


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
        _zmq_pub_ctx (zmq.Context): ZMQ context for publishing.
        _zmq_sub_ctx (zmq.Context): ZMQ context for subscribing.
        _zmq_pub (zmq.Socket): ZMQ socket for publishing.
        _zmq_sub (Optional[zmq.Socket]): ZMQ socket for subscribing.
        _message_class_cache (dict): Cache for message classes.
        _remote_message_ids (set): Set of remote message IDs.
        _receive_thread (threading.Thread): Thread for receiving messages.
    """

    allowed_message_in_types = [DAQJobMessage]  # accept all message types
    config_type = DAQJobRemoteConfig
    config: DAQJobRemoteConfig
    restart_offset = timedelta(seconds=5)

    _zmq_pub_ctx: zmq.Context
    _zmq_sub_ctx: zmq.Context

    _zmq_pub: zmq.Socket
    _zmq_sub: Optional[zmq.Socket]
    _message_class_cache: dict[str, type[DAQJobMessage]]
    _remote_message_ids: set[str]
    _receive_thread: threading.Thread

    def __init__(self, config: DAQJobRemoteConfig, **kwargs):
        super().__init__(config, **kwargs)

        self._zmq_pub_ctx = zmq.Context()
        self._logger.debug(f"Listening on {config.zmq_local_url}")
        self._zmq_pub = self._zmq_pub_ctx.socket(zmq.PUB)
        self._zmq_pub.bind(config.zmq_local_url)
        self._zmq_sub = None

        self._receive_thread = threading.Thread(
            target=self._start_receive_thread,
            args=(config.zmq_remote_urls,),
            daemon=True,
        )
        self._message_class_cache = {}

        self._message_class_cache = {
            x.__name__: x for x in DAQJobMessage.__subclasses__()
        }
        self._remote_message_ids = set()

    def handle_message(self, message: DAQJobMessage) -> bool:
        if (
            # Do not send stats messages to the remote
            isinstance(message, DAQJobMessageStats)
            # Ignore if we already received the message
            or message.id in self._remote_message_ids
            # Ignore if the message is not allowed by the DAQ Job
            or not super().handle_message(message)
            # Ignore if the message is remote, meaning it was sent by another Supervisor
            or message.is_remote
        ):
            return True  # Silently ignore

        if message.remote_config.remote_disable:
            return True

        remote_topic = message.remote_config.remote_topic or DEFAULT_REMOTE_TOPIC

        self._zmq_pub.send_multipart(
            [remote_topic.encode(), self._pack_message(message)]
        )
        self._logger.debug(
            f"Sent message '{type(message).__name__}' to topic '{remote_topic}'"
        )
        return True

    def _create_zmq_sub(self, remote_urls: list[str]) -> zmq.Socket:
        """
        Create a ZMQ subscriber socket.

        Args:
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
            recv_message = self._unpack_message(message)
            self._logger.debug(
                f"Received {len(message)} bytes for message '{type(recv_message).__name__}' on topic '{topic.decode()}'"
            )
            recv_message.is_remote = True
            # remote message_in -> message_out
            self.message_out.put(recv_message)

    def start(self):
        """
        Start the receive thread and the DAQ job.
        """
        self._receive_thread.start()

        while True:
            if not self._receive_thread.is_alive():
                raise RuntimeError("Receive thread died")
            # message_in -> remote message_out
            self.consume()
            time.sleep(0.1)

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

    def __del__(self):
        """
        Destructor for DAQJobRemote.
        """
        if getattr(self, "_zmq_sub_ctx", None) is not None:
            self._zmq_sub_ctx.destroy()
        if self._zmq_pub_ctx is not None:
            self._zmq_pub_ctx.destroy()

        return super().__del__()