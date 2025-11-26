import logging
import threading
from datetime import datetime
from typing import Optional

import msgspec
import zmq

from enrgdaq.cnc.handlers import (
    HeartbeatHandler,
    ReqListClientsHandler,
    ReqPingHandler,
    ReqRestartDAQJobsHandler,
    ReqRunCustomDAQJobHandler,
    ReqStatusHandler,
    ReqUpdateAndRestartHandler,
    ResPingHandler,
    ResStatusHandler,
)
from enrgdaq.cnc.models import (
    CNCMessage,
    CNCMessageHeartbeat,
    CNCMessageReqListClients,
    CNCMessageReqPing,
    CNCMessageReqRestartDAQJobs,
    CNCMessageReqRunCustomDAQJob,
    CNCMessageReqStatus,
    CNCMessageReqUpdateAndRestart,
    CNCMessageResPing,
    CNCMessageResStatus,
    CNCMessageType,
)
from enrgdaq.cnc.rest import start_rest_api
from enrgdaq.models import SupervisorCNCConfig

CNC_HEARTBEAT_INTERVAL_SECONDS = 1


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
        self.port = config.server_port

        self.context = zmq.Context()
        self.poller = zmq.Poller()

        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._rest_api_thread = None

        self.message_handlers = {
            CNCMessageHeartbeat: HeartbeatHandler(self),
            CNCMessageReqPing: ReqPingHandler(self),
            CNCMessageResPing: ResPingHandler(self),
            CNCMessageReqStatus: ReqStatusHandler(self),
            CNCMessageResStatus: ResStatusHandler(self),
            CNCMessageReqListClients: ReqListClientsHandler(self),
            CNCMessageReqUpdateAndRestart: ReqUpdateAndRestartHandler(self),
            CNCMessageReqRestartDAQJobs: ReqRestartDAQJobsHandler(self),
            CNCMessageReqRunCustomDAQJob: ReqRunCustomDAQJobHandler(self),
        }

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
                if current_time - last_heartbeat_time >= CNC_HEARTBEAT_INTERVAL_SECONDS:
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
        """Handles messages received on the server (from clients)."""
        try:
            sender_identity, *parts = self.socket.recv_multipart()
        except zmq.ZMQError:
            self._logger.warning("ZMQ error receiving message.")
            return

        if not parts:
            self._logger.debug("Received empty message.")
            return

        try:
            sender_id_str = sender_identity.decode("utf-8")
        except UnicodeDecodeError:
            self._logger.error("Received message with non-utf8 identity.")
            return

        # Update client info on any message received from registered client
        if sender_id_str in self.clients:
            self.clients[sender_id_str]["last_seen"] = datetime.now().isoformat()

        if len(parts) == 1:
            # Direct message from a client (like heartbeat) or response from client
            message_content = parts[0]
            try:
                msg = msgspec.msgpack.decode(message_content, type=CNCMessageType)
                self._logger.debug(
                    f"Server received direct message of type {type(msg).__name__} from {sender_id_str}"
                )

                # Update client info if it's a heartbeat
                if isinstance(msg, CNCMessageHeartbeat):
                    self.clients[sender_id_str] = {
                        "identity": sender_identity,
                        "last_seen": datetime.now().isoformat(),
                        "info": msg.supervisor_info,
                    }

                # Handle the message with appropriate handler
                handler = self.message_handlers.get(type(msg))
                if handler:
                    result = handler.handle(sender_identity, msg)
                    if result:
                        response, _ = result
                        self.send_message(response, sender_identity)
                else:
                    self._logger.warning(
                        f"Server received unhandled message type {type(msg).__name__} from {sender_id_str}"
                    )
            except Exception as e:
                self._logger.error(
                    f"Error processing direct message from {sender_id_str}: {e}"
                )
        elif len(parts) == 2:
            target_or_external_identity, message_content = parts

            # If the sender is a registered client, this is a response to forward back to external client
            if sender_id_str in self.clients:
                external_client_identity = target_or_external_identity
                response_message = message_content
                self._logger.debug(
                    f"Server forwarding response from client {sender_id_str} back to external client"
                )
                # Forward the response back to the external client
                self.socket.send_multipart([external_client_identity, response_message])
            else:
                # This is a forward request from an external client to a specific registered client
                try:
                    target_client_id = target_or_external_identity.decode("utf-8")
                    msg = msgspec.msgpack.decode(message_content, type=CNCMessageType)

                    self._logger.debug(
                        f"Server received forward request for {target_client_id} from {sender_id_str}"
                    )

                    # Check if target client exists
                    if target_client_id not in self.clients:
                        self._logger.error(f"Client '{target_client_id}' not found.")
                        return

                    # Forward the message to the target client
                    target_client_identity = self.clients[target_client_id]["identity"]
                    self.socket.send_multipart(
                        [target_client_identity, sender_identity, message_content]
                    )

                except UnicodeDecodeError:
                    self._logger.error(
                        "Received message with non-utf8 target client ID."
                    )
                except Exception as e:
                    self._logger.error(f"Error processing forward request: {e}")
        else:
            self._logger.warning(
                f"Received message with {len(parts)} parts from {sender_id_str} (expected 1 or 2)"
            )

    def handle_client_message(self):
        """Handles messages received on the client (from server)."""
        try:
            parts = self.socket.recv_multipart()
        except zmq.ZMQError:
            self._logger.warning("ZMQ error receiving message.")
            return

        if not parts:
            self._logger.debug("Received empty message.")
            return

        if len(parts) == 1:
            # Direct message from server
            message_content = parts[0]
            try:
                msg = msgspec.msgpack.decode(message_content, type=CNCMessageType)
                self._logger.debug(
                    f"Client received direct message of type {type(msg).__name__} from server"
                )

                # Handle the message with appropriate handler
                handler = self.message_handlers.get(type(msg))
                if handler:
                    # For client, pass the client's own identity to the handler
                    client_identity = self.socket.getsockopt_string(
                        zmq.IDENTITY
                    ).encode("utf-8")
                    result = handler.handle(client_identity, msg)
                    if result:
                        response, _ = result
                        self.send_message(response)
                else:
                    self._logger.warning(
                        f"Client received unhandled message type {type(msg).__name__} from server"
                    )
            except Exception as e:
                self._logger.error(f"Error processing direct message from server: {e}")
        elif len(parts) == 2:
            # Message forwarded from external client: [external_client_identity, message_content]
            external_client_identity, message_content = parts
            try:
                msg = msgspec.msgpack.decode(message_content, type=CNCMessageType)
                self._logger.debug(
                    f"Client received forwarded message of type {type(msg).__name__} from external client"
                )

                # Handle the message with appropriate handler
                handler = self.message_handlers.get(type(msg))
                if handler:
                    # Pass the external client's identity to the handler so it knows who to respond to
                    result = handler.handle(external_client_identity, msg)
                    if result:
                        response, _ = result
                        # Send response back to server, which will forward to external client
                        self.socket.send_multipart(
                            [external_client_identity, msgspec.msgpack.encode(response)]
                        )
                else:
                    self._logger.warning(
                        f"Client received unhandled forwarded message type {type(msg).__name__} from external client"
                    )
            except Exception as e:
                self._logger.error(
                    f"Error processing forwarded message from external client: {e}"
                )
        else:
            self._logger.warning(
                f"Client received message with {len(parts)} parts (expected 1 or 2)"
            )


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
        logging.getLogger(__name__).error(
            f"Failed to start SupervisorCNC: {e}", exc_info=True
        )
        return None
