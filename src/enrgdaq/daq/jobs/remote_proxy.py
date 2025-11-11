import zmq

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import DAQJobConfig


class DAQJobRemoteProxyConfig(DAQJobConfig):
    """
    Configuration for DAQJobRemoteProxy.

    Attributes:
        zmq_xsub_url (str): ZMQ xsub URL.
        zmq_xpub_url (str): ZMQ xpub URL.
    """

    zmq_xsub_url: str
    zmq_xpub_url: str


class DAQJobRemoteProxy(DAQJob):
    """
    DAQJobRemoteProxy is a DAQJob that acts as a proxy between two ZMQ sockets.
    It uses zmq.proxy to forward messages between xsub and xpub.

    pub -> xsub -> xpub -> sub

    When you want to the DAQJobRemoteProxy:
    - For pub, connect to xsub
    - For sub, connect to xpub

    Attributes:
        config_type (type): Configuration type for the job.
        config (DAQJobRemoteProxyConfig): Configuration instance.
    """

    config_type = DAQJobRemoteProxyConfig
    config: DAQJobRemoteProxyConfig

    def __init__(self, config: DAQJobRemoteProxyConfig, **kwargs):
        super().__init__(config, **kwargs)

        self._zmq_ctx = zmq.Context()
        self._xsub_sock = self._zmq_ctx.socket(zmq.XSUB)
        self._xsub_sock.bind(self.config.zmq_xsub_url)

        self._xpub_sock = self._zmq_ctx.socket(zmq.XPUB)
        self._xpub_sock.bind(self.config.zmq_xpub_url)

        self._logger.info(
            f"Proxying between {self.config.zmq_xsub_url} -> {self.config.zmq_xpub_url}"
        )

    def start(self):
        """
        Start the ZMQ proxy.
        """
        try:
            assert self._xsub_sock is not None and self._xpub_sock is not None
            zmq.proxy(self._xsub_sock, self._xpub_sock)
        except zmq.ContextTerminated:
            pass

    def __del__(self):
        """
        Destructor for DAQJobRemoteProxy.
        """
        if self._xsub_sock is not None:
            self._xsub_sock.close()
        if self._xpub_sock is not None:
            self._xpub_sock.close()
        if self._zmq_ctx is not None:
            self._zmq_ctx.destroy()

        return super().__del__()
