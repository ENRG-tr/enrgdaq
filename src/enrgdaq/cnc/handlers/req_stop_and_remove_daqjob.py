from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Tuple

from enrgdaq.cnc.handlers.base import CNCMessageHandler
from enrgdaq.cnc.models import (
    CNCMessage,
    CNCMessageReqStopAndRemoveDAQJob,
    CNCMessageResStopAndRemoveDAQJob,
)
from enrgdaq.daq.models import DAQJobMessageStop

if TYPE_CHECKING:
    from enrgdaq.cnc.base import SupervisorCNC


class ReqStopAndRemoveDAQJobHandler(CNCMessageHandler):
    """
    Handler for CNCMessageReqStopAndRemoveDAQJob messages.
    """

    def __init__(self, cnc: SupervisorCNC):
        """
        Initialize the handler.
        :param cnc: The SupervisorCNC instance.
        """
        super().__init__(cnc)

    def handle(
        self, sender_identity: bytes, msg: CNCMessageReqStopAndRemoveDAQJob
    ) -> Optional[Tuple[CNCMessage, bool]]:
        """
        Handles a stop and remove DAQJob request.
        :param sender_identity: The ZMQ identity of the message sender.
        :param msg: The stop and remove DAQJob request message.
        :return: A stop and remove DAQJob response message.
        """
        self._logger.info(
            f"Received stop and remove DAQJob request for job: {msg.daq_job_name}"
        )

        try:
            supervisor = self.cnc.supervisor
            if supervisor:
                # Find the DAQ job process with the specified name
                target_process = None
                for daq_job_process in supervisor.daq_job_processes:
                    if daq_job_process.daq_job_cls.__name__ == msg.daq_job_name:
                        target_process = daq_job_process
                        break

                if target_process:
                    # Send a stop message to the DAQ job process
                    try:
                        target_process.message_in.put(
                            DAQJobMessageStop(
                                reason="Stop and remove requested via CNC"
                            )
                        )

                        # Remove the process from the supervisor's list
                        supervisor.daq_job_processes.remove(target_process)
                        supervisor.daq_job_stats.pop(
                            target_process.daq_job_cls.__name__
                        )

                        success = True
                        message = f"Stop signal sent and DAQ job '{msg.daq_job_name}' removed from supervisor."
                        self._logger.info(message)
                    except Exception as e:
                        success = False
                        message = f"Error sending stop message to DAQ job '{msg.daq_job_name}': {str(e)}"
                        self._logger.error(message)
                else:
                    success = False
                    message = f"DAQ job '{msg.daq_job_name}' not found"
                    self._logger.warning(message)
            else:
                success = False
                message = "Supervisor not available"
                self._logger.error(message)

        except Exception as e:
            success = False
            message = (
                f"Error stopping and removing DAQJob '{msg.daq_job_name}': {str(e)}"
            )
            self._logger.error(message, exc_info=True)

        return CNCMessageResStopAndRemoveDAQJob(success=success, message=message), True
