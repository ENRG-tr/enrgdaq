import time
import unittest
from concurrent.futures import TimeoutError
from unittest.mock import MagicMock, patch

from enrgdaq.cnc.base import SupervisorCNC
from enrgdaq.cnc.models import (
    CNCMessageReqPing,
    CNCMessageReqRestartDAQ,
    CNCMessageReqRestartDAQJobs,
    CNCMessageReqRunCustomDAQJob,
    CNCMessageReqStatus,
    CNCMessageResPing,
    CNCMessageResRestartDAQ,
    CNCMessageResRestartDAQJobs,
    CNCMessageResRunCustomDAQJob,
    CNCMessageResStatus,
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
        self.daq_job_processes = []

    def get_status(self):
        return SupervisorStatus(
            supervisor_info=self.config.info,
            daq_job_stats=self.daq_job_stats,
            daq_job_remote_stats=self.daq_job_remote_stats,
            restart_schedules=[],
        )

    def stop(self):
        pass

    def start_daq_job_processes(self, *args, **kwargs):
        return []

    def restart_daq_jobs(self):
        pass


class TestCNC(unittest.TestCase):
    def setUp(self):
        # 1. Start Server
        self.server_supervisor = MockSupervisor("server", is_server=True)
        self.server_cnc = SupervisorCNC(
            supervisor=self.server_supervisor,
            config=SupervisorCNCConfig(is_server=True),
        )

        # 2. Start Client
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
        for _ in range(15):
            if "client1" in self.server_cnc.clients:
                break
            time.sleep(0.2)
        else:
            self.fail(
                f"Client 'client1' not found in server clients: {list(self.server_cnc.clients.keys())}"
            )

        client_info = self.server_cnc.clients["client1"]
        self.assertIsNotNone(client_info.info)
        self.assertEqual(client_info.info.supervisor_id, "client1")

    def test_ping_pong_sync(self):
        self.test_connection_and_heartbeat()

        ping_msg = CNCMessageReqPing()
        response = self.server_cnc.send_command_sync("client1", ping_msg, timeout=2)

        self.assertIsInstance(response, CNCMessageResPing)
        self.assertEqual(response.req_id, ping_msg.req_id)

    def test_get_status(self):
        self.test_connection_and_heartbeat()

        status_msg = CNCMessageReqStatus()
        response = self.server_cnc.send_command_sync("client1", status_msg, timeout=2)

        self.assertIsInstance(response, CNCMessageResStatus)
        self.assertEqual(response.status.supervisor_info.supervisor_id, "client1")

    @patch("enrgdaq.cnc.handlers.ReqRestartDAQJobsHandler.handle")
    def test_restart_daq(self, mock_handle):
        self.test_connection_and_heartbeat()

        # Added 'message' argument to satisfy msgspec definition
        mock_handle.return_value = (
            CNCMessageResRestartDAQ(success=True, message="OK"),
            None,
        )

        update_msg = CNCMessageReqRestartDAQ()
        response = self.server_cnc.send_command_sync("client1", update_msg, timeout=2)

        self.assertIsInstance(response, CNCMessageResRestartDAQ)
        self.assertTrue(response.success)

    @patch("enrgdaq.cnc.handlers.ReqRestartDAQJobsHandler.handle")
    def test_restart_daqjobs(self, mock_handle):
        self.test_connection_and_heartbeat()

        # Added 'message' argument here as well for safety
        mock_handle.return_value = (
            CNCMessageResRestartDAQJobs(success=True, message="OK"),
            None,
        )

        restart_msg = CNCMessageReqRestartDAQJobs()
        response = self.server_cnc.send_command_sync("client1", restart_msg, timeout=2)

        self.assertIsInstance(response, CNCMessageResRestartDAQJobs)
        self.assertTrue(response.success)

    @patch("enrgdaq.cnc.handlers.ReqRunCustomDAQJobHandler.handle")
    def test_run_custom_daqjob(self, mock_handle):
        self.test_connection_and_heartbeat()

        # Added 'message' argument
        mock_handle.return_value = (
            CNCMessageResRunCustomDAQJob(success=True, message="OK"),
            None,
        )

        config_str = "daq_job_type = 'test'"
        custom_msg = CNCMessageReqRunCustomDAQJob(config=config_str)
        response = self.server_cnc.send_command_sync("client1", custom_msg, timeout=3)

        self.assertIsInstance(response, CNCMessageResRunCustomDAQJob)
        self.assertTrue(response.success)

    def test_list_clients(self):
        self.test_connection_and_heartbeat()

        clients = self.server_cnc.clients
        self.assertIn("client1", clients)
        client_info = clients["client1"]
        self.assertIsNotNone(client_info.info)
        self.assertEqual(client_info.info.supervisor_id, "client1")

    def test_command_timeout(self):
        ping_msg = CNCMessageReqPing()
        with self.assertRaises(TimeoutError):
            self.server_cnc.send_command_sync("unknown_client", ping_msg, timeout=1)
        self.assertEqual(len(self.server_cnc._pending_responses), 0)

    def test_multiple_clients(self):
        client2_supervisor = MockSupervisor("client2")
        client2_cnc = SupervisorCNC(
            supervisor=client2_supervisor, config=SupervisorCNCConfig()
        )
        client2_cnc._logger = self.mock_logger
        client2_cnc.start()

        try:
            time.sleep(1.0)
            for _ in range(20):
                if (
                    "client1" in self.server_cnc.clients
                    and "client2" in self.server_cnc.clients
                ):
                    break
                time.sleep(0.2)
            else:
                self.fail("Not all clients connected")

            ping_msg = CNCMessageReqPing()
            resp1 = self.server_cnc.send_command_sync("client1", ping_msg, timeout=2)
            self.assertIsInstance(resp1, CNCMessageResPing)

            resp2 = self.server_cnc.send_command_sync("client2", ping_msg, timeout=2)
            self.assertIsInstance(resp2, CNCMessageResPing)

        finally:
            client2_cnc.stop()


if __name__ == "__main__":
    unittest.main()
