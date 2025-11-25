import logging
import threading
from datetime import datetime
from typing import Optional

import msgspec
import zmq

from enrgdaq.cnc.models import (
    CNCMessage,
    CNCMessageHeartbeat,
    CNCMessageReqListClients,
    CNCMessageReqPing,
    CNCMessageReqStatus,
    CNCMessageResListClients,
    CNCMessageResPing,
    CNCMessageResStatus,
    CNCMessageType,
)
from enrgdaq.cnc.rest import start_rest_api
from enrgdaq.models import SupervisorCNCConfig

CNC_DEFAULT_PORT = 5555


class SupervisorCNC:
    """
    Command and Control for ENRGDAQ Supervisor.
    This class can act as a server or a client.
    """

    socket: zmq.Socket

    def __init__(
        self,
        supervisor,
        config: SupervisorCNCConfig,
    ):
        from enrgdaq.supervisor import Supervisor

        self._logger = logging.getLogger(__name__)
        self.supervisor: Supervisor = supervisor
        self.supervisor_info = supervisor.config.info
        self.config = config
        self.is_server = config.is_server
        self.server_host = config.server_host
        self.port = CNC_DEFAULT_PORT

        self.context = zmq.Context()
        self.poller = zmq.Poller()

        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._rest_api_thread = None

        if self.is_server:
            self.clients = {}
            self.socket = self.context.socket(zmq.ROUTER)
            self.socket.bind(f"tcp://*:{self.port}")
            self._logger.info(f"C&C server started on port {self.port}")
            if self.config.rest_api_enabled:
                start_rest_api(
                    self.context,
                    self.port,
                    self.config.rest_api_host,
                    self.config.rest_api_port,
                )
        else:
            self.socket = self.context.socket(zmq.DEALER)
            self.socket.setsockopt_string(
                zmq.IDENTITY, self.supervisor_info.supervisor_id
            )
            self.socket.connect(f"tcp://{self.server_host}:{self.port}")
            self._logger.info(f"C&C client connected to {self.server_host}:{self.port}")

        self.poller.register(self.socket, zmq.POLLIN)

    def start(self):
        """Starts the C&C thread."""
        self._thread.start()

    def stop(self):
        """Stops the C&C thread."""
        self._stop_event.set()
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.close()
        self._thread.join(timeout=5)
        if self._thread.is_alive():
            self._logger.warning("C&C thread failed to stop.")
        self.context.term()

    def run(self):
        """Main loop for the C&C thread."""
        if not self.is_server:
            import time

            last_heartbeat_time = time.time()
            # Send initial heartbeat
            self.send_heartbeat()

        heartbeat_interval = 1

        while not self._stop_event.is_set():
            try:
                socks = dict(self.poller.poll(timeout=100))
            except zmq.ZMQError:
                if self._stop_event.is_set():
                    break  # Stop if event is set
                continue  # Continue if there's a ZMQ error but stop event is not set

            if self.socket in socks and socks[self.socket] == zmq.POLLIN:
                if self.is_server:
                    self.handle_server_message()
                else:
                    self.handle_client_message()

            # Send heartbeat periodically if client
            if not self.is_server:
                current_time = time.time()
                if current_time - last_heartbeat_time >= heartbeat_interval:
                    self.send_heartbeat()
                    last_heartbeat_time = current_time

    def send_heartbeat(self):
        """Client sends a heartbeat to the server."""
        if self.is_server:
            return
        self._logger.debug("Sending heartbeat")
        msg = CNCMessageHeartbeat(supervisor_info=self.supervisor_info)
        self.send_message(msg)

    def send_message(self, msg: CNCMessage, identity: Optional[bytes] = None):
        """Sends a message."""
        packed_msg = msgspec.msgpack.encode(msg)
        if self.is_server and identity:
            self.socket.send_multipart([identity, packed_msg])
        else:
            self.socket.send(packed_msg)

    def handle_server_message(self):
        """Handles messages received on the server."""
        try:
            sender_identity, *parts = self.socket.recv_multipart()
        except zmq.ZMQError:
            # Socket was likely closed during shutdown
            return

        if not parts:
            return  # Should not happen with ROUTER

        # Get sender ID as string
        try:
            sender_id_str = sender_identity.decode("utf-8")
        except UnicodeDecodeError:
            self._logger.error("Received message with non-utf8 identity.")
            return

        if len(parts) == 1:  # Direct message from a supervisor client or CLI
            message = parts[0]
            try:
                msg = msgspec.msgpack.decode(message, type=CNCMessageType)

                if isinstance(msg, CNCMessageHeartbeat):
                    self._logger.info(f"Received heartbeat from {sender_id_str}")
                    self.clients[sender_id_str] = {
                        "identity": sender_identity,
                        "last_seen": datetime.now().isoformat(),
                        "info": msg.supervisor_info,
                    }
                elif isinstance(msg, CNCMessageResPing):
                    self._logger.info(f"Received pong from {sender_id_str}")
                elif isinstance(msg, CNCMessageResStatus):
                    self._logger.info(
                        f"Received status from {sender_id_str}: {msg.status}"
                    )
                elif isinstance(msg, CNCMessageReqListClients):
                    self._logger.info(
                        f"Received list clients request from {sender_id_str}"
                    )
                    client_list = {
                        cid: cinfo["info"] for cid, cinfo in self.clients.items()
                    }
                    response = CNCMessageResListClients(clients=client_list)
                    self.send_message(response, sender_identity)
                else:
                    self._logger.warning(
                        f"Received unhandled direct message type from {sender_id_str}"
                    )
            except Exception as e:
                self._logger.error(
                    f"Error decoding direct message from {sender_id_str}: {e}"
                )

        elif len(parts) == 2:  # Forwarded message
            recipient_id, message = parts

            if sender_id_str in self.clients:
                # This is a reply from a client, forward to recipient
                # The recipient_id is the ZMQ identity of the CLI
                self.socket.send_multipart([recipient_id, message])
                # Also log the status message for testing purposes
                try:
                    msg = msgspec.msgpack.decode(message, type=CNCMessageType)
                    if isinstance(msg, CNCMessageResStatus):
                        self._logger.info(
                            f"Received status from {sender_id_str}: {msg.status}"
                        )
                except Exception:
                    pass
            else:
                # This is a request from a CLI, forward to a client
                try:
                    target_client_id_str = recipient_id.decode("utf-8")
                    if target_client_id_str in self.clients:
                        target_identity = self.clients[target_client_id_str]["identity"]
                        self.socket.send_multipart(
                            [target_identity, sender_identity, message]
                        )
                    else:
                        self._logger.error(
                            f"Client '{target_client_id_str}' not found."
                        )
                except UnicodeDecodeError:
                    self._logger.error("Received command with non-utf8 client ID.")

    def handle_client_message(self):
        """Handles messages received on the client."""
        try:
            parts = self.socket.recv_multipart()
        except zmq.ZMQError:
            # Socket was likely closed during shutdown
            return

        if len(parts) == 1:  # Direct command from server
            message = parts[0]
            try:
                msg = msgspec.msgpack.decode(message, type=CNCMessageType)
                if isinstance(msg, CNCMessageReqPing):
                    self._logger.info("Received ping, sending pong.")
                    self.send_message(CNCMessageResPing())
                elif isinstance(msg, CNCMessageReqStatus):
                    self._logger.info("Received get status request.")
                    status = self.supervisor.get_status()
                    self.send_message(CNCMessageResStatus(status=status))
                else:
                    self._logger.warning("Received unhandled message type from server")
            except Exception as e:
                self._logger.error(f"Error handling message: {e}")
        elif len(parts) == 2:  # Forwarded command from CLI
            cli_identity, message = parts
            try:
                msg = msgspec.msgpack.decode(message, type=CNCMessageType)
                if isinstance(msg, CNCMessageReqStatus):
                    self._logger.info("Received get status request from CLI.")
                    status = self.supervisor.get_status()
                    response = CNCMessageResStatus(status=status)
                    # Send response back to the server, addressed to the CLI
                    self.socket.send_multipart(
                        [cli_identity, msgspec.msgpack.encode(response)]
                    )
                elif isinstance(msg, CNCMessageReqPing):
                    self._logger.info("Received ping from CLI.")
                    self.send_message(CNCMessageResPing())
                else:
                    self._logger.warning("Received unhandled forwarded message")
            except Exception as e:
                self._logger.error(f"Error handling forwarded message: {e}")


def start_supervisor_cnc(
    supervisor,
    config: SupervisorCNCConfig,
) -> Optional[SupervisorCNC]:
    """
    Initializes and starts the SupervisorCNC.

    Args:
        supervisor (Supervisor): The supervisor instance.
        config (SupervisorCNCConfig): The C&C configuration.

    Returns:
        Optional[SupervisorCNC]: The started SupervisorCNC instance, or None on failure.
    """
    try:
        cnc = SupervisorCNC(
            supervisor=supervisor,
            config=config,
        )
        cnc.start()
        return cnc
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to start SupervisorCNC: {e}")
        return None
