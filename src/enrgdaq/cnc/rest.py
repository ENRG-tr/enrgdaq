import threading

import msgspec
import uvicorn
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel

from enrgdaq.cnc.models import (
    CNCMessageReqPing,
    CNCMessageReqRestartDAQ,
    CNCMessageReqRunCustomDAQJob,
    CNCMessageReqStatus,
    CNCMessageReqStopDAQJob,
    CNCMessageReqStopDAQJobs,
)


def start_rest_api(cnc_instance):
    from enrgdaq.cnc.base import CNC_MAX_CLIENT_LOGS

    """
    Starts the REST API server in a separate thread.
    Directly uses the passed `cnc_instance` to interact with the system.
    """
    app = FastAPI()

    # Helper to execute the sync command safely
    def execute_command(client_id: str, msg, timeout=5):
        try:
            # Check if client exists first
            if client_id not in cnc_instance.clients:
                raise HTTPException(
                    status_code=404, detail="Client not found or not connected."
                )

            reply = cnc_instance.send_command_sync(client_id, msg, timeout=timeout)
            return reply
        except TimeoutError:
            raise HTTPException(
                status_code=504, detail="Timeout waiting for client response."
            )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal Error: {str(e)}")

    @app.get("/clients")
    def list_clients():
        # Directly read the CNC registry
        return Response(
            content=msgspec.json.encode(cnc_instance.clients),
            media_type="application/json",
        )

    @app.post("/clients/{client_id}/ping")
    def ping_client(client_id: str):
        msg = CNCMessageReqPing()
        reply = execute_command(client_id, msg)
        return Response(
            content=msgspec.json.encode(reply), media_type="application/json"
        )

    @app.get("/clients/{client_id}/status")
    def get_status(client_id: str):
        msg = CNCMessageReqStatus()
        reply = execute_command(client_id, msg)
        # Handle reply structure (usually ResStatus has a .status field)
        return Response(
            content=msgspec.json.encode(reply.status), media_type="application/json"
        )

    class RestartDAQRequest(BaseModel):
        update: bool = False

    @app.post("/clients/{client_id}/restart_daq")
    def restart_daq(client_id: str, request: RestartDAQRequest):
        msg = CNCMessageReqRestartDAQ(update=request.update)
        reply = execute_command(client_id, msg)
        return Response(
            content=msgspec.json.encode(reply), media_type="application/json"
        )

    @app.post("/clients/{client_id}/stop_daqjobs")
    def stop_daqjobs_client(client_id: str):
        msg = CNCMessageReqStopDAQJobs()
        reply = execute_command(client_id, msg)
        return Response(
            content=msgspec.json.encode(reply), media_type="application/json"
        )

    class StopDAQJobRequest(BaseModel):
        daq_job_name: str
        remove: bool = False

    @app.post("/clients/{client_id}/stop_daqjob")
    def stop_daqjob_client(client_id: str, request: StopDAQJobRequest):
        msg = CNCMessageReqStopDAQJob(daq_job_name=request.daq_job_name)
        reply = execute_command(client_id, msg)
        return Response(
            content=msgspec.json.encode(reply), media_type="application/json"
        )

    class RunCustomDAQJobRequest(BaseModel):
        config: str

    @app.post("/clients/{client_id}/run_custom_daqjob")
    def run_custom_daqjob_client(client_id: str, request: RunCustomDAQJobRequest):
        msg = CNCMessageReqRunCustomDAQJob(config=request.config)
        reply = execute_command(client_id, msg)
        return Response(
            content=msgspec.json.encode(reply), media_type="application/json"
        )

    # Logging Endpoints
    @app.get("/clients/{client_id}/logs")
    def get_logs(client_id: str):
        if client_id not in cnc_instance.client_logs:
            return Response(
                content=msgspec.json.encode({"error": "Client not found"}),
                media_type="application/json",
            )
        logs = list(cnc_instance.client_logs[client_id])
        return Response(
            content=msgspec.json.encode({"logs": logs[-CNC_MAX_CLIENT_LOGS:]}),
            media_type="application/json",
        )

    config = uvicorn.Config(
        app,
        host=cnc_instance.config.rest_api_host,
        port=cnc_instance.config.rest_api_port,
        log_level="info",
    )
    server = uvicorn.Server(config)
    rest_api_thread = threading.Thread(target=server.run, daemon=True)
    rest_api_thread.start()
    return rest_api_thread
