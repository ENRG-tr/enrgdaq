import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from daq.jobs.remote import DAQJobRemote, DAQJobRemoteConfig
from daq.jobs.store.csv import DAQJobStoreConfigCSV
from daq.jobs.test_job import DAQJobTest
from daq.models import DEFAULT_REMOTE_TOPIC, DAQJobMessage, DAQRemoteConfig
from daq.store.models import DAQJobMessageStore, DAQJobStoreConfig


class TestDAQJobRemote(unittest.TestCase):
    @patch("daq.jobs.remote.zmq.Context")
    def setUp(self, MockZmqContext):
        self.mock_context = MockZmqContext.return_value
        self.mock_sender = self.mock_context.socket.return_value
        self.mock_receiver = self.mock_context.socket.return_value
        self.config = DAQJobRemoteConfig(
            daq_job_type="remote",
            zmq_local_url="tcp://localhost:5555",
            zmq_remote_urls=["tcp://localhost:5556"],
        )
        self.daq_job_remote = DAQJobRemote(self.config)
        self.daq_job_remote._zmq_pub = self.mock_sender

    def test_handle_message(self):
        message = DAQJobMessage(
            id="testmsg",
        )
        self.daq_job_remote.handle_message(message)
        self.mock_sender.send_multipart.assert_called_once_with(
            [DEFAULT_REMOTE_TOPIC.encode(), self.daq_job_remote._pack_message(message)]
        )

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
        message = DAQJobMessageStore(
            id="testmsg",
            store_config=DAQJobStoreConfig(
                csv=DAQJobStoreConfigCSV(file_path="test", add_date=True)
            ),
            data=[],
            keys=[],
            daq_job_info=DAQJobTest({"daq_job_type": "test"}).info,
        )
        self.daq_job_remote.message_out = MagicMock()

        call_count = 0

        def side_effect():
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise RuntimeError("Stop receive thread")
            return [
                DEFAULT_REMOTE_TOPIC.encode(),
                self.daq_job_remote._pack_message(message),
            ]

        self.daq_job_remote._create_zmq_sub = MagicMock(return_value=self.mock_receiver)
        self.mock_receiver.recv_multipart.side_effect = side_effect

        with self.assertRaises(RuntimeError):
            self.daq_job_remote._start_receive_thread(["tcp://localhost:5556"])

        assert_msg = message
        assert_msg.is_remote = True
        self.daq_job_remote.message_out.put.assert_called_once_with(assert_msg)
        self.assertEqual(self.daq_job_remote.message_out.put.call_count, 1)
        self.assertEqual(call_count, 2)

    def test_handle_message_with_remote_config(self):
        message = DAQJobMessage(
            id="testmsg",
            remote_config=DAQRemoteConfig(remote_topic="custom_topic"),
        )
        self.daq_job_remote.handle_message(message)
        self.mock_sender.send_multipart.assert_called_once_with(
            ["custom_topic".encode(), self.daq_job_remote._pack_message(message)]
        )

    def test_handle_message_with_remote_disable(self):
        message = DAQJobMessage(
            id="testmsg",
            remote_config=DAQRemoteConfig(remote_disable=True),
        )
        result = self.daq_job_remote.handle_message(message)
        self.assertTrue(result)
        self.mock_sender.send_multipart.assert_not_called()

    def test_handle_message_with_default_remote_topic(self):
        message = DAQJobMessage(
            id="testmsg",
            remote_config=DAQRemoteConfig(),
        )
        self.daq_job_remote.handle_message(message)
        self.mock_sender.send_multipart.assert_called_once_with(
            [DEFAULT_REMOTE_TOPIC.encode(), self.daq_job_remote._pack_message(message)]
        )

    def test_handle_message_with_custom_remote_topic(self):
        message = DAQJobMessage(
            id="testmsg",
            remote_config=DAQRemoteConfig(remote_topic="custom_topic"),
        )
        self.daq_job_remote.handle_message(message)
        self.mock_sender.send_multipart.assert_called_once_with(
            ["custom_topic".encode(), self.daq_job_remote._pack_message(message)]
        )


if __name__ == "__main__":
    unittest.main()
