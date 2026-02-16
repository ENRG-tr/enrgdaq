from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Tuple

import msgspec

from enrgdaq.cnc.handlers.base import CNCMessageHandler
from enrgdaq.cnc.models import (
    CNCMessage,
    CNCMessageReqSendMessage,
    CNCMessageResSendMessage,
)
from enrgdaq.daq.models import DAQJobMessage
from enrgdaq.daq.topics import Topic

if TYPE_CHECKING:
    from enrgdaq.cnc.base import SupervisorCNC


class ReqSendMessageHandler(CNCMessageHandler):
    """
    Handler for CNCMessageReqSendMessage messages.
    Sends a custom message to DAQ job(s).
    """

    def __init__(self, cnc: SupervisorCNC):
        """
        Initialize the handler.
        :param cnc: The SupervisorCNC instance.
        """
        super().__init__(cnc)

    def handle(
        self, sender_identity: bytes, msg: CNCMessageReqSendMessage
    ) -> Optional[Tuple[CNCMessage, bool]]:
        """
        Handles a send custom message request.
        :param sender_identity: The ZMQ identity of the message sender.
        :param msg: The send custom message request message.
        :return: A send custom message response message.
        """
        self._logger.info(
            f"Received send message request: type={msg.message_type}, "
            f"target={msg.target_daq_job_unique_id or 'all'}"
        )

        try:
            # Get all registered DAQJobMessage types
            message_types = self._get_message_types()

            # Find the requested message type
            if msg.message_type not in message_types:
                return CNCMessageResSendMessage(
                    success=False,
                    message=f"Unknown message type: {msg.message_type}. "
                    f"Available types: {list(message_types.keys())}",
                    jobs_notified=0,
                ), True

            message_cls = message_types[msg.message_type]

            # Decode the JSON payload
            try:
                message_instance: DAQJobMessage = msgspec.json.decode(
                    msg.payload.encode(), type=message_cls
                )
            except Exception as e:
                return CNCMessageResSendMessage(
                    success=False,
                    message=f"Failed to decode payload as {msg.message_type}: {str(e)}",
                    jobs_notified=0,
                ), True

            supervisor = self.cnc.supervisor
            if not supervisor:
                return CNCMessageResSendMessage(
                    success=False,
                    message="Supervisor not available",
                    jobs_notified=0,
                ), True

            jobs_notified = 0

            # Send to specific job or all jobs
            for daq_job_process in supervisor.daq_job_processes:
                # If a target is specified, only send to that job
                if msg.target_daq_job_unique_id:
                    if (
                        daq_job_process.daq_job_info
                        and daq_job_process.daq_job_info.unique_id
                        != msg.target_daq_job_unique_id
                    ):
                        continue

                # Check if the job accepts this message type
                daq_job_cls = daq_job_process.daq_job_cls
                accepts_message = any(
                    isinstance(message_instance, msg_type)
                    for msg_type in daq_job_cls.allowed_message_in_types
                )

                if not accepts_message or daq_job_process.daq_job_info is None:
                    continue

                # Set the topic for the message as the target daq job id
                message_instance.topics.add(
                    Topic.daq_job_direct(daq_job_process.daq_job_info.unique_id)
                )

                # Send the message
                try:
                    self.cnc.supervisor.message_broker.send(message_instance)
                    jobs_notified += 1
                    self._logger.debug(
                        f"Sent {msg.message_type} to {daq_job_cls.__name__}"
                    )
                except Exception as e:
                    self._logger.warning(
                        f"Failed to send message to {daq_job_cls.__name__}: {e}"
                    )

            if jobs_notified == 0:
                return CNCMessageResSendMessage(
                    success=False,
                    message=f"No jobs accepted message type {msg.message_type}"
                    + (
                        f" (target: {msg.target_daq_job_unique_id})"
                        if msg.target_daq_job_unique_id
                        else ""
                    ),
                    jobs_notified=0,
                ), True

            return CNCMessageResSendMessage(
                success=True,
                message=f"Message sent to {jobs_notified} job(s)",
                jobs_notified=jobs_notified,
            ), True

        except Exception as e:
            self._logger.error(f"Error sending custom message: {e}", exc_info=True)
            return CNCMessageResSendMessage(
                success=False,
                message=f"Error sending message: {str(e)}",
                jobs_notified=0,
            ), True

    def _get_message_types(self) -> dict[str, type]:
        """
        Gets all registered DAQJobMessage types.
        Returns a dict mapping type name to type class.
        """
        from enrgdaq.daq.jobs.caen.hv import DAQJobMessageCAENHVSetChParam
        from enrgdaq.daq.models import (
            DAQJobMessage,
            DAQJobMessageHeartbeat,
            DAQJobMessageStatsRemote,
            DAQJobMessageStop,
        )
        from enrgdaq.daq.store.models import (
            DAQJobMessageStore,
            DAQJobMessageStoreRaw,
            DAQJobMessageStoreTabular,
        )

        # Build a dictionary of all known message types
        types: dict[str, type] = {
            "DAQJobMessage": DAQJobMessage,
            "DAQJobMessageHeartbeat": DAQJobMessageHeartbeat,
            "DAQJobMessageStop": DAQJobMessageStop,
            "DAQJobMessageStore": DAQJobMessageStore,
            "DAQJobMessageStoreRaw": DAQJobMessageStoreRaw,
            "DAQJobMessageStoreTabular": DAQJobMessageStoreTabular,
            "DAQJobMessageStatsRemote": DAQJobMessageStatsRemote,
            "DAQJobMessageCAENHVSetChParam": DAQJobMessageCAENHVSetChParam,
        }

        return types
