from __future__ import annotations

import tempfile
from typing import TYPE_CHECKING, Optional, Tuple

from enrgdaq.cnc.handlers.base import CNCMessageHandler
from enrgdaq.cnc.models import (
    CNCMessage,
    CNCMessageReqRunCustomDAQJob,
    CNCMessageResRunCustomDAQJob,
)

if TYPE_CHECKING:
    from enrgdaq.cnc.base import SupervisorCNC


class ReqRunCustomDAQJobHandler(CNCMessageHandler):
    """
    Handler for CNCMessageReqRunCustomDAQJob messages.
    """

    def __init__(self, cnc: SupervisorCNC):
        """
        Initialize the handler.
        :param cnc: The SupervisorCNC instance.
        """
        super().__init__(cnc)

    def handle(
        self, sender_identity: bytes, msg: CNCMessageReqRunCustomDAQJob
    ) -> Optional[Tuple[CNCMessage, bool]]:
        """
        Handles a run custom DAQJob request.
        :param sender_identity: The ZMQ identity of the message sender.
        :param msg: The run custom DAQJob request message.
        :return: A run custom DAQJob response message.
        """
        self._logger.info("Received run custom DAQJob request.")

        try:
            from enrgdaq.daq.daq_job import build_daq_job
            from enrgdaq.models import SupervisorInfo

            # Create a basic supervisor info for the custom job
            supervisor_info = SupervisorInfo(supervisor_id="remote_custom")

            # Write the config to a temporary file
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".toml", delete=False
            ) as temp_config:
                temp_config.write(msg.config)
                config_path = temp_config.name

            # Read the config from the temporary file as bytes
            with open(config_path, "rb") as f:
                config_data = f.read()

            try:
                # Build the DAQ job from the config
                daq_job_process = build_daq_job(config_data, supervisor_info)

                self._logger.info(
                    f"Starting DAQ job: {daq_job_process.daq_job_cls.__name__}"
                )

                # Start the DAQ job
                self.cnc.supervisor.start_daq_job_processes([daq_job_process])

                success = True
                message = f"DAQ job started with PID: {daq_job_process.process.pid if daq_job_process.process else 'Unknown'}"
                self._logger.info(message)

            except Exception as e:
                success = False
                message = f"Error starting DAQ job: {str(e)}"
                self._logger.error(message)
            finally:
                # Clean up the temporary file
                import os

                os.unlink(config_path)

        except Exception as e:
            success = False
            message = f"Error processing run custom DAQJob request: {str(e)}"
            self._logger.error(message)

        return CNCMessageResRunCustomDAQJob(success=success, message=message), True
