import unittest
from unittest.mock import patch

import zmq

from enrgdaq.daq.jobs.remote_proxy import DAQJobRemoteProxy, DAQJobRemoteProxyConfig


class TestDAQJobRemoteProxy(unittest.TestCase):
    @patch("enrgdaq.daq.jobs.remote_proxy.zmq.Context")
    def setUp(self, MockZmqContext):
        self.mock_context = MockZmqContext.return_value
        self.config = DAQJobRemoteProxyConfig(
            daq_job_type="remote_proxy",
            zmq_xsub_url="tcp://localhost:5557",
            zmq_xpub_url="tcp://localhost:5558",
        )
        self.daq_job_remote_proxy = DAQJobRemoteProxy(self.config)

    def test_initialization(self):
        self.mock_context.socket.assert_any_call(zmq.XSUB)
        self.mock_context.socket.assert_any_call(zmq.XPUB)
        self.mock_context.socket.return_value.bind.assert_any_call(
            "tcp://localhost:5557"
        )
        self.mock_context.socket.return_value.bind.assert_any_call(
            "tcp://localhost:5558"
        )
        self.assertEqual(self.daq_job_remote_proxy.config, self.config)
        self.assertEqual(
            self.daq_job_remote_proxy._xsub_sock, self.mock_context.socket.return_value
        )
        self.assertEqual(
            self.daq_job_remote_proxy._xpub_sock, self.mock_context.socket.return_value
        )

    @patch("enrgdaq.daq.jobs.remote_proxy.zmq.proxy")
    def test_start(self, mock_zmq_proxy):
        self.daq_job_remote_proxy.start()
        mock_zmq_proxy.assert_called_once_with(
            self.daq_job_remote_proxy._xsub_sock,
            self.daq_job_remote_proxy._xpub_sock,
        )

    def test_destructor(self):
        self.daq_job_remote_proxy.__del__()
        self.mock_context.socket.return_value.close.assert_called()
        self.mock_context.destroy.assert_called_once()


if __name__ == "__main__":
    unittest.main()
