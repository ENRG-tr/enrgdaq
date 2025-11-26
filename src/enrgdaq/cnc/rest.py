import random
import string
import threading

import msgspec
import uvicorn
import zmq
from fastapi import FastAPI, HTTPException

from enrgdaq.cnc.models import (
    CNCMessageReqListClients,
    CNCMessageType,
)


def start_rest_api(context, port, rest_api_host, rest_api_port):
    app = FastAPI()

    # Create a single persistent socket for the REST API client
    # This avoids creating new sockets for each request.
    client_identity = f"cnc-rest-{''.join(random.choices(string.ascii_lowercase + string.digits, k=8))}"
    persistent_socket = context.socket(zmq.DEALER)
    persistent_socket.setsockopt_string(zmq.IDENTITY, client_identity)
    persistent_socket.connect(f"tcp://localhost:{port}")
    socket_lock = threading.Lock()

    async def send_request(msg: CNCMessageType, timeout: int = 2000):
        """Helper to send a request and wait for a reply."""
        encoded_msg = msgspec.msgpack.encode(msg)
        with socket_lock:
            poller = zmq.Poller()
            poller.register(persistent_socket, zmq.POLLIN)
            persistent_socket.send(encoded_msg)

            socks = dict(poller.poll(timeout))
            if persistent_socket in socks:
                message = persistent_socket.recv()
                return msgspec.msgpack.decode(message, type=CNCMessageType)
            else:
                raise HTTPException(
                    status_code=504, detail="Timeout waiting for reply from server."
                )

    @app.get("/clients")
    async def list_clients():
        msg = CNCMessageReqListClients()
        reply = await send_request(msg)
        return reply.clients

    @app.post("/clients/{client_id}/ping")
    async def ping_client(client_id: str):
        # In the new architecture, the server handles routing to specific clients
        # We'll need to implement a mechanism to send to specific clients
        # For now, this would require updating the message system to support targeted commands
        raise HTTPException(
            status_code=501,
            detail="Client-specific commands not implemented in simplified architecture",
        )

    @app.get("/clients/{client_id}/status")
    async def get_status(client_id: str):
        # In the new architecture, the server handles routing to specific clients
        # We'll need to implement a mechanism to send to specific clients
        # For now, this would require updating the message system to support targeted commands
        raise HTTPException(
            status_code=501,
            detail="Client-specific commands not implemented in simplified architecture",
        )

    # A single socket is not thread-safe with async frameworks like FastAPI.
    # We need a dependency to manage access to the socket.
    # However, for simplicity in this context, we'll use a lock and assume
    # the performance implications are acceptable.
    # A more robust solution might involve a pool of sockets or a dedicated request/reply thread.

    # The previous implementation of get_socket() per request is problematic
    # because it creates a new identity and connection for every API call,
    # which is inefficient and can lead to issues.

    # Let's adapt the old structure to use the new helpers to minimize changes.
    # NOTE: The code below is now redundant due to the helpers above, but I am
    # adapting it to show how the logic changes in the original structure.
    # The best approach is to use the helpers `send_request` and `forward_request`.

    def get_socket_DEPRECATED():
        socket = context.socket(zmq.DEALER)
        client_identity = f"cnc-rest-{''.join(random.choices(string.ascii_lowercase + string.digits, k=8))}"
        socket.setsockopt_string(zmq.IDENTITY, client_identity)
        socket.connect(f"tcp://localhost:{port}")
        return socket

    config = uvicorn.Config(
        app,
        host=rest_api_host,
        port=rest_api_port,
        log_level="info",
    )
    server = uvicorn.Server(config)
    rest_api_thread = threading.Thread(target=server.run, daemon=True)
    rest_api_thread.start()
    return rest_api_thread
