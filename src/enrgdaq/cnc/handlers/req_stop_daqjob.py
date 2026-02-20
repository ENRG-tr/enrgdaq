from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Tuple

from enrgdaq.cnc.handlers.base import CNCMessageHandler
from enrgdaq.cnc.models import (
    CNCMessage,
    CNCMessageReqStopDAQJob,
    CNCMessageResStopDAQJob,
)
from enrgdaq.daq.models import DAQJobMessageStop
from enrgdaq.daq.topics import Topic

if TYPE_CHECKING:
    from enrgdaq.cnc.base import SupervisorCNC


class ReqStopDAQJobHandler(CNCMessageHandler):
    """
    Handler for CNCMessageReqStopDAQJob messages.
    """

    def __init__(self, cnc: SupervisorCNC):
        """
        Initialize the handler.
        :param cnc: The SupervisorCNC instance.
        """
        super().__init__(cnc)

    def handle(
        self, sender_identity: bytes, msg: CNCMessageReqStopDAQJob
    ) -> Optional[Tuple[CNCMessage, bool]]:
        """
        Handles a stop and remove DAQJob request.
        :param sender_identity: The ZMQ identity of the message sender.
        :param msg: The stop and remove DAQJob request message.
        :return: A stop and remove DAQJob response message.
        """
        if not msg.daq_job_name and not msg.daq_job_unique_id:
            raise Exception(
                "Either daq_job_name or daq_job_unique_id must be specified"
            )
        self._logger.info(
            f"Received stop and remove DAQJob request for job: {msg.daq_job_unique_id}"
        )

        try:
            supervisor = self.cnc.supervisor
            if not supervisor:
                message = "Supervisor not available"
                self._logger.error(message)
                return CNCMessageResStopDAQJob(success=False, message=message), True

            # Find the DAQ job process with the specified name
            target_process = None
            for daq_job_process in supervisor.daq_job_processes:
                # If daq_job_unique_id is specified
                if (
                    msg.daq_job_unique_id
                    and daq_job_process.daq_job_info
                    and daq_job_process.daq_job_info.unique_id == msg.daq_job_unique_id
                ):
                    target_process = daq_job_process
                    break
                # If daq_job_name is specified
                elif (
                    msg.daq_job_name
                    and daq_job_process.daq_job_cls.__name__ == msg.daq_job_name
                ):
                    target_process = daq_job_process
                    break

            if not target_process:
                message = f"DAQ job with unique id '{msg.daq_job_unique_id}' not found"
                self._logger.warning(message)
                return CNCMessageResStopDAQJob(success=False, message=message), True

            # Send a stop message to the DAQ job process
            assert target_process.daq_job_info is not None
            self.cnc.supervisor.message_broker.send(
                DAQJobMessageStop(
                    reason="Stop and remove requested via CNC",
                    topics={
                        Topic.daq_job_direct(target_process.daq_job_info.unique_id)
                    },
                )
            )

            # Remove the process from the supervisor's list
            supervisor.daq_job_processes.remove(target_process)
            if (
                msg.remove
                and target_process.daq_job_cls.__name__ in supervisor.daq_job_stats
            ):
                supervisor.daq_job_stats.pop(target_process.daq_job_cls.__name__)

            message = f"DAQJob {msg.daq_job_unique_id} " + (
                "removed and stopped" if msg.remove else "stopped"
            )
            self._logger.info(message)
            return CNCMessageResStopDAQJob(success=True, message=message), True

        except Exception as e:
            message = f"Error stopping and removing DAQJob '{msg.daq_job_unique_id}': {str(e)}"
            self._logger.error(message, exc_info=True)
            return CNCMessageResStopDAQJob(success=False, message=message), True
