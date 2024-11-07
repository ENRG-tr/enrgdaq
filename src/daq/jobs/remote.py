import pickle
import threading
import time
from typing import Optional

import msgspec
import zmq

from daq.base import DAQJob
from daq.jobs.handle_stats import DAQJobMessageStats
from daq.models import DAQJobConfig, DAQJobMessage

DAQ_JOB_REMOTE_MAX_REMOTE_MESSAGE_ID_COUNT = 10000


class DAQJobRemoteConfig(DAQJobConfig):
    zmq_local_url: str
    zmq_remote_urls: list[str]


class DAQJobRemote(DAQJob):
    """
    DAQJobRemote is a DAQJob that connects two seperate ENRGDAQ instances.
    It sends to and receives from a remote ENRGDAQ, in such that:

    - message_in -> remote message_out
    - remote message_in -> message_out

    TODO: Use zmq CURVE security
    """

    allowed_message_in_types = [DAQJobMessage]  # accept all message types
    config_type = DAQJobRemoteConfig
    config: DAQJobRemoteConfig
    _zmq_pub: zmq.Socket
    _zmq_sub: Optional[zmq.Socket]
    _message_class_cache: dict[str, type[DAQJobMessage]]
    _remote_message_ids: set[str]
    _receive_thread: threading.Thread

    def __init__(self, config: DAQJobRemoteConfig):
        super().__init__(config)
        self._zmq_context = zmq.Context()
        self._logger.debug(f"Listening on {config.zmq_local_url}")
        self._zmq_pub = self._zmq_context.socket(zmq.PUB)
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
            isinstance(message, DAQJobMessageStats)
            or message.id in self._remote_message_ids
            or not super().handle_message(message)
            or message.is_remote
        ):
            return True  # Silently ignore

        self._zmq_pub.send(self._pack_message(message))
        return True

    def _create_zmq_sub(self, remote_urls: list[str]):
        ctx = zmq.Context()
        zmq_sub = ctx.socket(zmq.SUB)
        for remote_url in remote_urls:
            self._logger.debug(f"Connecting to {remote_url}")
            zmq_sub.connect(remote_url)
            zmq_sub.subscribe("")
        return zmq_sub

    def _start_receive_thread(self, remote_urls: list[str]):
        self._zmq_sub = self._create_zmq_sub(remote_urls)

        while True:
            message = self._zmq_sub.recv()
            self._logger.debug(f"Received {len(message)} bytes")
            recv_message = self._unpack_message(message)
            recv_message.is_remote = True
            # remote message_in -> message_out
            self.message_out.put(recv_message)

    def start(self):
        self._receive_thread.start()

        while True:
            if not self._receive_thread.is_alive():
                raise RuntimeError("Receive thread died")
            # message_in -> remote message_out
            self.consume()
            time.sleep(0.1)

    def _pack_message(self, message: DAQJobMessage, use_pickle: bool = True) -> bytes:
        message_type = type(message).__name__
        self._logger.debug(f"Packing message {message_type} ({message.id})")
        if use_pickle:
            return pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)

        return msgspec.msgpack.encode([message_type, message])

    def _unpack_message(self, message: bytes) -> DAQJobMessage:
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
        self._logger.debug(f"Unpacked message {message_type} ({res.id})")
        return res

    def __del__(self):
        if self._zmq_sub is not None:
            self._zmq_sub.close()
        self._zmq_pub.close()

        return super().__del__()
