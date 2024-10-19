import pickle
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from daq.jobs.remote import DAQJobRemote, DAQJobRemoteConfig
from daq.jobs.test_job import DAQJobTest
from daq.store.models import DAQJobMessageStore


class TestDAQJobRemote(unittest.TestCase):
    @patch("daq.jobs.remote.zmq.Context")
    def setUp(self, MockZmqContext):
        self.mock_context = MockZmqContext.return_value
        self.mock_sender = self.mock_context.socket.return_value
        self.mock_receiver = self.mock_context.socket.return_value
        self.config = DAQJobRemoteConfig(
            daq_job_type="remote",
            zmq_sender_url="tcp://localhost:5555",
            zmq_receiver_url="tcp://localhost:5556",
        )
        self.daq_job_remote = DAQJobRemote(self.config)
        self.daq_job_remote._zmq_sender = self.mock_sender
        self.daq_job_remote._zmq_receiver = self.mock_receiver

    def test_handle_message(self):
        message = DAQJobMessageStore(
            store_config={}, data=[], keys=[], daq_job=DAQJobTest({})
        )
        self.daq_job_remote.handle_message(message)
        self.mock_sender.send.assert_called_once_with(pickle.dumps(message))

    def test_start(self):
        mock_receive_thread = MagicMock()

        def stop_receive_thread():
            time.sleep(0.1)
            mock_receive_thread.is_alive.return_value = False

        self.daq_job_remote._receive_thread = mock_receive_thread
        threading.Thread(target=stop_receive_thread, daemon=True).start()

        with self.assertRaises(RuntimeError):
            self.daq_job_remote.start()

    def test_receive_thread(self):
        message = DAQJobMessageStore(store_config={}, data=[], keys=[], daq_job=None)  # type: ignore
        self.daq_job_remote.message_out = MagicMock()

        call_count = 0

        def side_effect():
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise Exception("Stop receive thread")
            return pickle.dumps(message)

        self.mock_receiver.recv.side_effect = side_effect

        with self.assertRaises(Exception):
            self.daq_job_remote._start_receive_thread()
        self.daq_job_remote.message_out.put.assert_called_once_with(message)
        self.assertEqual(self.daq_job_remote.message_out.put.call_count, 1)
        self.assertEqual(call_count, 2)


if __name__ == "__main__":
    unittest.main()
