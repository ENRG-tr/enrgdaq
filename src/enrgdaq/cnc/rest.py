import random
import string
import threading

import msgspec
import uvicorn
import zmq
from fastapi import FastAPI, HTTPException

from enrgdaq.cnc.models import (
    CNCMessageReqListClients,
    CNCMessageReqPing,
    CNCMessageReqStatus,
    CNCMessageResListClients,
    CNCMessageResPing,
    CNCMessageResStatus,
    CNCMessageType,
)


def start_rest_api(context, port, rest_api_host, rest_api_port):
    app = FastAPI()

    def get_socket():
        socket = context.socket(zmq.DEALER)
        client_identity = f"cnc-rest-{''.join(random.choices(string.ascii_lowercase + string.digits, k=8))}"
        socket.setsockopt_string(zmq.IDENTITY, client_identity)
        socket.connect(f"tcp://localhost:{port}")
        return socket

    @app.get("/clients")
    async def list_clients():
        socket = get_socket()
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)

        msg = CNCMessageReqListClients()
        socket.send(msgspec.msgpack.encode(msg))

        socks = dict(poller.poll(2000))
        if socket in socks:
            message = socket.recv()
            try:
                reply = msgspec.msgpack.decode(message, type=CNCMessageType)
                if isinstance(reply, CNCMessageResListClients):
                    return reply.clients
                else:
                    raise HTTPException(
                        status_code=500, detail="Invalid response from server"
                    )
            except Exception as e:
                raise HTTPException(
                    status_code=500, detail=f"Error decoding reply: {e}"
                )
        else:
            raise HTTPException(
                status_code=504, detail="Timeout waiting for reply from server."
            )

    @app.post("/clients/{client_id}/ping")
    async def ping_client(client_id: str):
        socket = get_socket()
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)

        msg = CNCMessageReqPing()
        socket.send_multipart([client_id.encode(), msgspec.msgpack.encode(msg)])

        socks = dict(poller.poll(2000))
        if socket in socks:
            message = socket.recv()
            try:
                reply = msgspec.msgpack.decode(message, type=CNCMessageType)
                if isinstance(reply, CNCMessageResPing):
                    return {"status": "pong"}
                else:
                    raise HTTPException(
                        status_code=500, detail="Invalid response from server"
                    )
            except Exception as e:
                raise HTTPException(
                    status_code=500, detail=f"Error decoding reply: {e}"
                )
        else:
            raise HTTPException(
                status_code=504, detail="Timeout waiting for reply from server."
            )

    @app.get("/clients/{client_id}/status")
    async def get_status(client_id: str):
        socket = get_socket()
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)

        msg = CNCMessageReqStatus()
        socket.send_multipart([client_id.encode(), msgspec.msgpack.encode(msg)])

        socks = dict(poller.poll(2000))
        if socket in socks:
            message = socket.recv()
            try:
                reply = msgspec.msgpack.decode(message, type=CNCMessageType)
                if isinstance(reply, CNCMessageResStatus):
                    return reply.status
                else:
                    raise HTTPException(
                        status_code=500, detail="Invalid response from server"
                    )
            except Exception as e:
                raise HTTPException(
                    status_code=500, detail=f"Error decoding reply: {e}"
                )
        else:
            raise HTTPException(
                status_code=504, detail="Timeout waiting for reply from server."
            )

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
