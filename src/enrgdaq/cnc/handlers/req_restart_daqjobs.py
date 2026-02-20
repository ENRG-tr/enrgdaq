from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Tuple

from enrgdaq.cnc.handlers.base import CNCMessageHandler
from enrgdaq.cnc.models import (
    CNCMessage,
    CNCMessageReqStopDAQJobs,
    CNCMessageResStopDAQJobs,
)
from enrgdaq.daq.models import DAQJobMessageStop
from enrgdaq.daq.topics import Topic

if TYPE_CHECKING:
    from enrgdaq.cnc.base import SupervisorCNC


class ReqStopDAQJobsHandler(CNCMessageHandler):
    """
    Handler for CNCMessageReqStopDAQJobs messages.
    """

    def __init__(self, cnc: SupervisorCNC):
        """
        Initialize the handler.
        :param cnc: The SupervisorCNC instance.
        """
        super().__init__(cnc)

    def handle(
        self, sender_identity: bytes, msg: CNCMessageReqStopDAQJobs
    ) -> Optional[Tuple[CNCMessage, bool]]:
        """
        Handles a restart DAQJobs request.
        :param sender_identity: The ZMQ identity of the message sender.
        :param msg: The restart DAQJobs request message.
        :return: A restart DAQJobs response message.
        """
        self._logger.info("Received restart DAQJobs request.")

        try:
            # Use the supervisor's built-in mechanism to restart DAQ jobs
            # by stopping all current processes which will trigger restart logic
            supervisor = self.cnc.supervisor
            if not supervisor:
                message = "Supervisor not available"
                self._logger.error(message)
                return CNCMessageResStopDAQJobs(success=False, message=message), True

            # Send stop messages to all DAQ job processes to trigger restart
            for daq_job_process in supervisor.daq_job_processes:
                try:
                    assert daq_job_process.daq_job_info, "DAQ job info not available"
                    self.cnc.supervisor.message_broker.send(
                        DAQJobMessageStop(
                            reason="Restart requested via CNC",
                            topics={
                                Topic.daq_job_direct(
                                    daq_job_process.daq_job_info.unique_id
                                )
                            },
                        )
                    )
                except Exception as e:
                    self._logger.warning(f"Error sending stop message to DAQ job: {e}")

            message = "Stop signals sent to all DAQ jobs."
            self._logger.info(message)
            return CNCMessageResStopDAQJobs(success=True, message=message), True

        except Exception as e:
            message = f"Error restarting DAQJobs: {str(e)}"
            self._logger.error(message)
            return CNCMessageResStopDAQJobs(success=False, message=message), True
