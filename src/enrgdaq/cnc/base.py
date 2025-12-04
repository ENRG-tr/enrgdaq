import logging
import queue
import threading
import time
import uuid
from concurrent.futures import Future
from datetime import datetime
from typing import Dict, Optional

import msgspec
import zmq

from enrgdaq.cnc.handlers import (
    HeartbeatHandler,
    ReqListClientsHandler,
    ReqPingHandler,
    ReqRestartDAQJobsHandler,
    ReqRestartHandler,
    ReqRunCustomDAQJobHandler,
    ReqStatusHandler,
    ReqStopAndRemoveDAQJobHandler,
    ResPingHandler,
    ResStatusHandler,
)
from enrgdaq.cnc.models import (
    CNCClientInfo,
    CNCMessage,
    CNCMessageHeartbeat,
    CNCMessageReqListClients,
    CNCMessageReqPing,
    CNCMessageReqRestartDAQ,
    CNCMessageReqRestartDAQJobs,
    CNCMessageReqRunCustomDAQJob,
    CNCMessageReqStatus,
    CNCMessageReqStopAndRemoveDAQJob,
    CNCMessageResPing,
    CNCMessageResStatus,
    CNCMessageType,
)
from enrgdaq.cnc.rest import start_rest_api
from enrgdaq.models import SupervisorCNCConfig

CNC_HEARTBEAT_INTERVAL_SECONDS = 1


class SupervisorCNC:
    """
    Simplified Command and Control.
    Handles ZMQ communication and provides a direct interface for the REST API.
    """

    def __init__(self, supervisor, config: SupervisorCNCConfig):
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(config.verbosity.to_logging_level())

        from enrgdaq.supervisor import Supervisor

        self.supervisor: Supervisor = supervisor
        self.config = config
        self.is_server = config.is_server
        self.supervisor_info = supervisor.config.info

        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.socket: Optional[zmq.Socket] = None

        self.clients: Dict[str, CNCClientInfo] = {}
        self._pending_responses: Dict[str, Future] = {}
        self._command_queue: queue.Queue = queue.Queue()

        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self.run, daemon=True)

        self.message_handlers = {
            CNCMessageHeartbeat: HeartbeatHandler(self),
            CNCMessageReqPing: ReqPingHandler(self),
            CNCMessageResPing: ResPingHandler(self),
            CNCMessageReqStatus: ReqStatusHandler(self),
            CNCMessageResStatus: ResStatusHandler(self),
            CNCMessageReqListClients: ReqListClientsHandler(self),
            CNCMessageReqRestartDAQ: ReqRestartHandler(self),
            CNCMessageReqRestartDAQJobs: ReqRestartDAQJobsHandler(self),
            CNCMessageReqRunCustomDAQJob: ReqRunCustomDAQJobHandler(self),
            CNCMessageReqStopAndRemoveDAQJob: ReqStopAndRemoveDAQJobHandler(self),
        }

        if self.is_server:
            self.socket = self.context.socket(zmq.ROUTER)
            self.socket.bind(f"tcp://*:{config.server_port}")
            self._logger.info(f"C&C Server started on port {config.server_port}")

            if config.rest_api_enabled:
                start_rest_api(self)
        else:
            self.socket = self.context.socket(zmq.DEALER)
            self.socket.setsockopt_string(
                zmq.IDENTITY, self.supervisor_info.supervisor_id
            )
            self.socket.connect(f"tcp://{config.server_host}:{config.server_port}")
            self._logger.info(
                f"C&C Client connected to {config.server_host}:{config.server_port}"
            )

        self.poller.register(self.socket, zmq.POLLIN)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self.socket:
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
        self._thread.join(timeout=2)
        self.context.term()

    def send_command_sync(
        self, target_client_id: str, msg: CNCMessage, timeout: int = 2
    ) -> CNCMessage:
        """
        Thread-safe method called by REST API to send a command and wait for a reply.
        """
        if not self.is_server:
            raise RuntimeError("Only server can send commands to clients.")

        # Generate request ID
        req_id = str(uuid.uuid4())
        msg.req_id = req_id

        future = Future()
        # Map the ID to the Future
        self._pending_responses[req_id] = future

        # Queue the send operation
        self._command_queue.put((target_client_id, msg))

        try:
            return future.result(timeout=timeout)
        except Exception as e:
            # Cleanup on timeout
            self._pending_responses.pop(req_id, None)
            raise e

    def run(self):
        last_heartbeat = 0
        while not self._stop_event.is_set():
            try:
                # 1. Process outgoing commands from REST API
                while not self._command_queue.empty():
                    target_id, msg = self._command_queue.get()
                    self._send_zmq_message(target_id.encode("utf-8"), msg)

                # 2. Poll for incoming ZMQ messages
                socks = dict(self.poller.poll(timeout=50))
                if self.socket in socks:
                    self.handle_incoming_message()

                # 3. Client Heartbeat
                if not self.is_server:
                    if time.time() - last_heartbeat >= CNC_HEARTBEAT_INTERVAL_SECONDS:
                        self._send_zmq_message(
                            None,
                            CNCMessageHeartbeat(supervisor_info=self.supervisor_info),
                        )
                        last_heartbeat = time.time()

            except zmq.ZMQError:
                if self._stop_event.is_set():
                    break
            except Exception as e:
                self._logger.error(f"Error in CNC loop: {e}", exc_info=True)

    def handle_incoming_message(self):
        assert self.socket is not None
        try:
            frames = self.socket.recv_multipart()
        except zmq.ZMQError:
            return

        if not frames:
            return

        # Router: [SenderID, Body]
        # Dealer: [Body]
        if self.is_server:
            if len(frames) != 2:
                return
            sender_id_bytes, body = frames[0], frames[1]
            sender_id_str = sender_id_bytes.decode("utf-8", errors="ignore")
        else:
            if len(frames) != 1:
                return
            sender_id_bytes = None
            sender_id_str = "server"
            body = frames[0]

        try:
            msg = msgspec.msgpack.decode(body, type=CNCMessageType)

            if self.is_server and sender_id_str:
                # Update Registry
                existing_info = self.clients.get(sender_id_str)
                self.clients[sender_id_str] = CNCClientInfo(
                    identity=sender_id_bytes,
                    last_seen=datetime.now().isoformat(),
                    info=msg.supervisor_info
                    if hasattr(msg, "supervisor_info")
                    else existing_info.info
                    if existing_info
                    else None,
                )

                # Only if the message actually has a req_id and it's in our pending map
                if (
                    hasattr(msg, "req_id")
                    and msg.req_id
                    and msg.req_id in self._pending_responses
                ):
                    future = self._pending_responses.pop(msg.req_id)
                    if not future.done():
                        future.set_result(msg)

            self._process_payload(msg, sender_id_bytes)

        except Exception as e:
            self._logger.error(f"Error processing payload: {e}", exc_info=True)

    def _process_payload(self, msg: CNCMessage, sender_id_bytes: Optional[bytes]):
        handler = self.message_handlers.get(type(msg))
        if handler:
            identity_arg = sender_id_bytes if self.is_server else b"server"
            result = handler.handle(identity_arg, msg)
            if result:
                response_msg, _ = result

                # Copy the request id from Request to Response
                # This ensures the Server knows which request this response belongs to.
                if hasattr(msg, "req_id") and hasattr(response_msg, "req_id"):
                    response_msg.req_id = msg.req_id

                self._send_zmq_message(sender_id_bytes, response_msg)

    def _send_zmq_message(self, identity: Optional[bytes], msg: CNCMessage):
        assert self.socket is not None
        packed = msgspec.msgpack.encode(msg)
        if self.is_server:
            if identity:
                self.socket.send_multipart([identity, packed])
        else:
            self.socket.send(packed)


def start_supervisor_cnc(
    supervisor, config: SupervisorCNCConfig
) -> Optional[SupervisorCNC]:
    try:
        cnc = SupervisorCNC(supervisor, config)
        cnc.start()
        return cnc
    except Exception as e:
        logging.getLogger(__name__).error(
            f"Failed to start SupervisorCNC: {e}", exc_info=True
        )
        return None
