import pickle
import threading
import time
from dataclasses import dataclass

import zmq

from daq.base import DAQJob
from daq.models import DAQJobConfig, DAQJobMessage


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

    def __init__(self, config: DAQJobRemoteConfig):
        super().__init__(config)
        self._zmq_context = zmq.Context()
        self._zmq_local = self._zmq_context.socket(zmq.PUSH)
        self._zmq_remote = self._zmq_context.socket(zmq.PULL)
        self._zmq_local.connect(config.zmq_local_url)
        self._zmq_remote.connect(config.zmq_remote_url)

        self._receive_thread = threading.Thread(
            target=self._start_receive_thread, daemon=True
        )

    def handle_message(self, message: DAQJobMessage) -> bool:
        print(type(message))
        self._zmq_local.send(pickle.dumps(message))
        return True

    def _start_receive_thread(self):
        while True:
            message = self._zmq_remote.recv()
            # remote message_in -> message_out
            self.message_out.put(pickle.loads(message))

    def start(self):
        self._receive_thread.start()

        while True:
            if not self._receive_thread.is_alive():
                raise RuntimeError("receive thread died")
            # message_in -> remote message_out
            self.consume()
            time.sleep(0.1)
