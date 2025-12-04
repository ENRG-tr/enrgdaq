from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Tuple

from enrgdaq.cnc.handlers.base import CNCMessageHandler
from enrgdaq.cnc.models import CNCMessage, CNCMessageReqStatus, CNCMessageResStatus

if TYPE_CHECKING:
    from enrgdaq.cnc.base import SupervisorCNC


class ReqStatusHandler(CNCMessageHandler):
    """
    Handler for CNCMessageReqStatus messages.
    """

    def __init__(self, cnc: SupervisorCNC):
        """
        Initialize the handler.
        :param cnc: The SupervisorCNC instance.
        """
        super().__init__(cnc)

    def handle(
        self, sender_identity: bytes, msg: CNCMessageReqStatus
    ) -> Optional[Tuple[CNCMessage, bool]]:
        """
        Handles a status request.
        :param sender_identity: The ZMQ identity of the message sender.
        :param msg: The status request message.
        :return: A status response message.
        """
        self._logger.info("Received get status request.")
        status = self.cnc.supervisor.get_status()
        return CNCMessageResStatus(status=status), True
