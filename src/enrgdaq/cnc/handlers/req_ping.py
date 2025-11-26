from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Tuple

from enrgdaq.cnc.handlers.base import CNCMessageHandler
from enrgdaq.cnc.models import CNCMessage, CNCMessageReqPing, CNCMessageResPing

if TYPE_CHECKING:
    from enrgdaq.cnc.base import SupervisorCNC


class ReqPingHandler(CNCMessageHandler):
    """
    Handler for CNCMessageReqPing messages.
    """

    def __init__(self, cnc: SupervisorCNC):
        """
        Initialize the handler.
        :param cnc: The SupervisorCNC instance.
        """
        super().__init__(cnc)

    def handle(
        self, sender_identity: bytes, msg: CNCMessageReqPing
    ) -> Optional[Tuple[CNCMessage, bool]]:
        """
        Handles a ping request.
        :param sender_identity: The ZMQ identity of the message sender.
        :param msg: The ping request message.
        :return: A pong response message.
        """
        self._logger.info("Received ping, sending pong.")
        return CNCMessageResPing(), True
