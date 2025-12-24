import logging
import os
import pickle
import tempfile
import uuid
from queue import Empty, Full
from typing import Any, Optional

import zmq

logger = logging.getLogger(__name__)


class ZMQQueue:
    """A high-performance multiprocess queue using ZeroMQ IPC.

    This class mimics the queue.Queue interface but works across process boundaries
    without requiring inheritance. It uses ZMQ PUSH/PULL sockets over IPC.

    The first call to 'get' will bind a PULL socket to the URI.
    The first call to 'put' will connect a PUSH socket to the URI.
    """

    def __init__(self, uri: Optional[str] = None, maxsize: int = 0):
        self.uri = uri or f"ipc://{tempfile.gettempdir()}/enrgdaq_q_{uuid.uuid4()}.ipc"
        self.maxsize = maxsize
        self._context: Optional[zmq.Context] = None
        self._push_socket: Optional[zmq.Socket] = None
        self._pull_socket: Optional[zmq.Socket] = None
        self._pid = os.getpid()

    def __getstate__(self):
        # Only pickle the URI and maxsize. Sockets and contexts cannot be pickled.
        return {
            "uri": self.uri,
            "maxsize": self.maxsize,
        }

    def __setstate__(self, state):
        self.uri = state["uri"]
        self.maxsize = state["maxsize"]
        self._context = None
        self._push_socket = None
        self._pull_socket = None
        self._pid = os.getpid()

    def _get_context(self) -> zmq.Context:
        if self._context is None or self._pid != os.getpid():
            self._context = zmq.Context.instance()
            self._pid = os.getpid()
        return self._context

    def _get_push_socket(self) -> zmq.Socket:
        if self._push_socket is None or self._pid != os.getpid():
            ctx = self._get_context()
            socket = ctx.socket(zmq.PUSH)
            # Set a high water mark if maxsize is specified
            if self.maxsize > 0:
                socket.set(zmq.SNDHWM, self.maxsize)
            socket.connect(self.uri)
            self._push_socket = socket

        assert self._push_socket is not None
        return self._push_socket

    def _get_pull_socket(self) -> zmq.Socket:
        if self._pull_socket is None or self._pid != os.getpid():
            ctx = self._get_context()
            socket = ctx.socket(zmq.PULL)
            if self.maxsize > 0:
                socket.set(zmq.RCVHWM, self.maxsize)
            try:
                socket.bind(self.uri)
            except zmq.ZMQError as e:
                if e.errno == zmq.EADDRINUSE:
                    # Someone else already bound to this URI (probably another consumer)
                    # For a simple Queue substitute, we should probably allow multiple consumers
                    # but ZMQ PULL doesn't naturally support multiple binders.
                    # However, if we're substituting Manager().Queue(), usually there's one consumer.
                    socket.connect(self.uri)
                else:
                    raise
            self._pull_socket = socket

        assert self._pull_socket is not None
        return self._pull_socket

    def put(self, obj: Any, block: bool = True, timeout: Optional[float] = None):
        socket = self._get_push_socket()
        flags = 0 if block else zmq.NOBLOCK

        try:
            buffers = []
            # Use Protocol 5 to collect out-of-band buffers for zero-copy
            header = pickle.dumps(obj, protocol=5, buffer_callback=buffers.append)

            # Combine header and buffers into a multipart message
            payload = [header] + [zmq.Frame(b) for b in buffers]

            if timeout is not None and block:
                if socket.poll(int(timeout * 1000), zmq.POLLOUT):
                    socket.send_multipart(payload, flags)
                else:
                    raise Full
            else:
                socket.send_multipart(payload, flags)
        except zmq.Again:
            raise Full

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        socket = self._get_pull_socket()

        try:
            if not block:
                payload = socket.recv_multipart(zmq.NOBLOCK)
            elif timeout is not None:
                if socket.poll(int(timeout * 1000), zmq.POLLIN):
                    payload = socket.recv_multipart()
                else:
                    raise Empty
            else:
                payload = socket.recv_multipart()

            header = payload[0]
            buffers = payload[1:]
            return pickle.loads(header, buffers=buffers)
        except (zmq.Again, IndexError):
            raise Empty

    def put_nowait(self, obj: Any):
        return self.put(obj, block=False)

    def get_nowait(self) -> Any:
        return self.get(block=False)

    def qsize(self) -> int:
        # ZMQ doesn't have a cross-platform way to get queue size from sockets easily
        # Returning 0 as a placeholder since supervisor uses it for stats on non-Mac
        return 0

    def empty(self) -> bool:
        socket = self._get_pull_socket()
        return not socket.poll(0, zmq.POLLIN)

    def close(self):
        if self._push_socket:
            self._push_socket.close()
            self._push_socket = None
        if self._pull_socket:
            self._pull_socket.close()
            self._pull_socket = None
        # Note: we don't destroy the URI/IPC file here as other processes might be using it.
        # Ideally, there should be a cleanup mechanism.

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def __repr__(self):
        return f"ZMQQueue(uri='{self.uri}', maxsize={self.maxsize})"
