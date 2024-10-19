import json
import threading
import time
from dataclasses import dataclass

import zmq

from daq.base import DAQJob
from daq.jobs.handle_stats import DAQJobMessageStats
from daq.models import DAQJobConfig, DAQJobMessage
from daq.store.models import DAQJobMessageStore

DAQ_JOB_REMOTE_MAX_REMOTE_MESSAGE_ID_COUNT = 1000


@dataclass
class DAQJobRemoteConfig(DAQJobConfig):
    zmq_local_url: str
    zmq_remote_url: str


class DAQJobRemote(DAQJob):
    """
    DAQJobRemote is a DAQJob that connects two seperate ENRGDAQ instances.
    It sends to and receives from a remote ENRGDAQ, in such that:

    - message_in -> remote message_out
    - remote message_in -> message_out
    """

    allowed_message_in_types = [DAQJobMessage]  # accept all message types
    config_type = DAQJobRemoteConfig
    config: DAQJobRemoteConfig
    _zmq_local: zmq.Socket
    _zmq_remote: zmq.Socket
    _message_class_cache: dict
    _remote_message_ids: set[str]

    def __init__(self, config: DAQJobRemoteConfig):
        super().__init__(config)
        self._zmq_context = zmq.Context()
        self._zmq_local = self._zmq_context.socket(zmq.PUSH)
        self._zmq_remote = self._zmq_context.socket(zmq.PULL)
        self._zmq_local.connect(config.zmq_local_url)
        self._zmq_remote.connect(config.zmq_remote_url)
        self._message_class_cache = {}

        self._receive_thread = threading.Thread(
            target=self._start_receive_thread, daemon=True
        )
        self._message_class_cache = {
            x.__name__: x for x in DAQJobMessage.__subclasses__()
        }
        self._remote_message_ids = set()

    def handle_message(self, message: DAQJobMessage) -> bool:
        if (
            isinstance(message, DAQJobMessageStats)
            or isinstance(
                message, DAQJobMessageStore
            )  # TODO: we should be able to send store messages
            or message.id in self._remote_message_ids
            or not super().handle_message(message)
        ):
            return False

        self._zmq_local.send(self._pack_message(message))
        return True

    def _start_receive_thread(self):
        while True:
            message = self._zmq_remote.recv()
            # remote message_in -> message_out
            self.message_out.put(self._unpack_message(message))

    def start(self):
        self._receive_thread.start()

        while True:
            if not self._receive_thread.is_alive():
                raise RuntimeError("receive thread died")
            # message_in -> remote message_out
            self.consume()
            time.sleep(0.1)

    def _pack_message(self, message: DAQJobMessage) -> bytes:
        message_type = type(message).__name__
        return json.dumps([message_type, message.to_json()]).encode("utf-8")

    def _unpack_message(self, message: bytes) -> DAQJobMessage:
        message_type, data = json.loads(message.decode("utf-8"))
        if message_type in self._message_class_cache:
            message_class = self._message_class_cache[message_type]
        else:
            message_class = globals()[message_type]
            self._message_class_cache[message_type] = message_class

        if not issubclass(message_class, DAQJobMessage):
            raise Exception(f"Invalid message type: {message_type}")

        res = message_class.from_json(data)
        assert res.id is not None, "Message id is not set"
        self._remote_message_ids.add(res.id)
        if len(self._remote_message_ids) > DAQ_JOB_REMOTE_MAX_REMOTE_MESSAGE_ID_COUNT:
            self._remote_message_ids.pop()
        return res
