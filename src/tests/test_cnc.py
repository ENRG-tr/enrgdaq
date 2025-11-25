import time
import unittest
from unittest.mock import MagicMock

import msgspec
import zmq

from enrgdaq.cnc.base import SupervisorCNC
from enrgdaq.cnc.models import (
    CNCMessageReqListClients,
    CNCMessageReqPing,
    CNCMessageReqStatus,
    CNCMessageResListClients,
    SupervisorStatus,
)
from enrgdaq.models import SupervisorCNCConfig, SupervisorConfig, SupervisorInfo


class MockSupervisor:
    def __init__(self, supervisor_id, is_server=False, server_host="localhost"):
        self.config = SupervisorConfig(
            info=SupervisorInfo(supervisor_id=supervisor_id),
            cnc=SupervisorCNCConfig(is_server=is_server, server_host=server_host),
        )
        self.daq_job_stats = {}
        self.daq_job_remote_stats = {}
        self.restart_schedules = []

    def get_status(self):
        return SupervisorStatus(
            supervisor_info=self.config.info,
            daq_job_stats=self.daq_job_stats,
            daq_job_remote_stats=self.daq_job_remote_stats,
            restart_schedules=[],
        )


class TestCNC(unittest.TestCase):
    def setUp(self):
        self.server_supervisor = MockSupervisor("server", is_server=True)
        self.server_cnc = SupervisorCNC(
            supervisor=self.server_supervisor,
            config=SupervisorCNCConfig(is_server=True),
        )

        self.client_supervisor = MockSupervisor("client1")
        self.client_cnc = SupervisorCNC(
            supervisor=self.client_supervisor, config=SupervisorCNCConfig()
        )

        self.mock_logger = MagicMock()
        self.server_cnc._logger = self.mock_logger
        self.client_cnc._logger = self.mock_logger

        self.server_cnc.start()
        self.client_cnc.start()
        time.sleep(0.5)

    def tearDown(self):
        self.server_cnc.stop()
        self.client_cnc.stop()

    def test_connection_and_heartbeat(self):
        time.sleep(0.5)

        # Wait for the heartbeat to be processed with retries
        for _ in range(
            15
        ):  # Try up to 15 times with 0.2 second intervals (3 seconds total)
            if "client1" in self.server_cnc.clients:
                break
            time.sleep(0.2)
        else:
            # If we still don't have the client after retries, fail the test
            self.fail(
                f"Client 'client1' not found in server clients: {list(self.server_cnc.clients.keys())}"
            )

        self.assertEqual(
            self.server_cnc.clients["client1"]["info"].supervisor_id, "client1"
        )

    def test_ping_pong(self):
        # Mock a CLI client
        cli_socket = self.server_cnc.context.socket(zmq.DEALER)
        try:
            cli_socket.setsockopt_string(zmq.IDENTITY, "cli-tester")
            cli_socket.connect(f"tcp://localhost:{self.server_cnc.port}")

            # Send a ping command from CLI to client1
            ping_msg = CNCMessageReqPing()
            cli_socket.send_multipart([b"client1", msgspec.msgpack.encode(ping_msg)])

            pong_received = False
            for _ in range(10):  # Poll for 1 second
                log_messages = [
                    call.args[0] for call in self.mock_logger.info.call_args_list
                ]
                if any("Received pong" in msg for msg in log_messages):
                    pong_received = True
                    break
                time.sleep(0.1)

            self.assertTrue(pong_received, "Did not receive pong in time")

            log_messages = [
                call.args[0] for call in self.mock_logger.info.call_args_list
            ]
            self.assertTrue(any("Received ping" in msg for msg in log_messages))
        finally:
            cli_socket.close()

    def test_get_status(self):
        # Mock a CLI client
        cli_socket = self.server_cnc.context.socket(zmq.DEALER)
        try:
            cli_socket.setsockopt_string(zmq.IDENTITY, "cli-tester-status")
            cli_socket.connect(f"tcp://localhost:{self.server_cnc.port}")

            # Send a get status command from CLI to client1
            status_msg = CNCMessageReqStatus()
            cli_socket.send_multipart([b"client1", msgspec.msgpack.encode(status_msg)])

            status_received = False
            for _ in range(10):  # Poll for 1 second
                log_messages = [
                    call.args[0] for call in self.mock_logger.info.call_args_list
                ]
                if any("Received status from client1" in msg for msg in log_messages):
                    status_received = True
                    break
                time.sleep(0.1)

            self.assertTrue(status_received, "Did not receive status in time")

            log_messages = [
                call.args[0] for call in self.mock_logger.info.call_args_list
            ]
            self.assertTrue(
                any("Received get status request" in msg for msg in log_messages)
            )
        finally:
            cli_socket.close()

    def test_list_clients(self):
        # Mock a CLI client
        cli_socket = self.server_cnc.context.socket(zmq.DEALER)
        try:
            cli_socket.setsockopt_string(zmq.IDENTITY, "cli-tester-list")
            cli_socket.connect(f"tcp://localhost:{self.server_cnc.port}")

            # Send a list clients command
            list_msg = CNCMessageReqListClients()
            cli_socket.send(msgspec.msgpack.encode(list_msg))

            # Wait for reply
            self.assertTrue(cli_socket.poll(1000))
            reply_msg = cli_socket.recv()
            reply = msgspec.msgpack.decode(reply_msg, type=CNCMessageResListClients)

            self.assertIn("client1", reply.clients)
            self.assertEqual(reply.clients["client1"].supervisor_id, "client1")
        finally:
            cli_socket.close()

    def test_get_status_for_unknown_client(self):
        # Mock a CLI client
        cli_socket = self.server_cnc.context.socket(zmq.DEALER)
        try:
            cli_socket.setsockopt_string(zmq.IDENTITY, "cli-tester-unknown")
            cli_socket.connect(f"tcp://localhost:{self.server_cnc.port}")

            # Send a get status command to a non-existent client
            status_msg = CNCMessageReqStatus()
            cli_socket.send_multipart(
                [b"unknown_client", msgspec.msgpack.encode(status_msg)]
            )

            # Check for the error log
            error_logged = False
            for _ in range(10):  # Poll for 1 second
                log_messages = [
                    call.args[0] for call in self.mock_logger.error.call_args_list
                ]
                if any(
                    "Client 'unknown_client' not found" in msg for msg in log_messages
                ):
                    error_logged = True
                    break
                time.sleep(0.1)

            self.assertTrue(error_logged, "Did not log error for unknown client")
        finally:
            cli_socket.close()

    def test_multiple_clients(self):
        # Set up a second client
        client2_supervisor = MockSupervisor("client2")
        client2_cnc = SupervisorCNC(
            supervisor=client2_supervisor, config=SupervisorCNCConfig()
        )
        client2_cnc._logger = self.mock_logger
        client2_cnc.start()
        try:
            # Wait for both clients to connect
            time.sleep(1.0)
            for _ in range(15):
                if (
                    "client1" in self.server_cnc.clients
                    and "client2" in self.server_cnc.clients
                ):
                    break
                time.sleep(0.2)
            else:
                self.fail(
                    f"Not all clients connected: {list(self.server_cnc.clients.keys())}"
                )

            # Test list_clients with multiple clients
            cli_socket = self.server_cnc.context.socket(zmq.DEALER)
            cli_socket.setsockopt_string(zmq.IDENTITY, "cli-tester-multi")
            cli_socket.connect(f"tcp://localhost:{self.server_cnc.port}")

            try:
                list_msg = CNCMessageReqListClients()
                cli_socket.send(msgspec.msgpack.encode(list_msg))

                self.assertTrue(cli_socket.poll(1000))
                reply_msg = cli_socket.recv()
                reply = msgspec.msgpack.decode(reply_msg, type=CNCMessageResListClients)

                self.assertIn("client1", reply.clients)
                self.assertIn("client2", reply.clients)
                self.assertEqual(reply.clients["client2"].supervisor_id, "client2")
            finally:
                cli_socket.close()
        finally:
            client2_cnc.stop()


if __name__ == "__main__":
    unittest.main()
