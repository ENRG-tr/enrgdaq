from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Tuple

from enrgdaq.cnc.handlers.base import CNCMessageHandler
from enrgdaq.cnc.models import CNCMessage, CNCMessageResPing

if TYPE_CHECKING:
    from enrgdaq.cnc.base import SupervisorCNC


class ResPingHandler(CNCMessageHandler):
    """
    Handler for CNCMessageResPing messages.
    """

    def __init__(self, cnc: SupervisorCNC):
        """
        Initialize the handler.
        :param cnc: The SupervisorCNC instance.
        """
        super().__init__(cnc)

    def handle(
        self, sender_identity: bytes, msg: CNCMessageResPing
    ) -> Optional[Tuple[CNCMessage, bool]]:
        """
        Handles a pong response.
        :param sender_identity: The ZMQ identity of the message sender.
        :param msg: The pong response message.
        :return: None
        """
        sender_id_str = sender_identity.decode("utf-8")
        self._logger.info(f"Received pong from {sender_id_str}")
        return None
