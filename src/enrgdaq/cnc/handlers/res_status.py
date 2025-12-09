from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Tuple

from enrgdaq.cnc.handlers.base import CNCMessageHandler
from enrgdaq.cnc.models import CNCMessage, CNCMessageResStatus

if TYPE_CHECKING:
    from enrgdaq.cnc.base import SupervisorCNC


class ResStatusHandler(CNCMessageHandler):
    """
    Handler for CNCMessageResStatus messages.
    """

    def __init__(self, cnc: SupervisorCNC):
        """
        Initialize the handler.
        :param cnc: The SupervisorCNC instance.
        """
        super().__init__(cnc)

    def handle(
        self, sender_identity: bytes, msg: CNCMessageResStatus
    ) -> Optional[Tuple[CNCMessage, bool]]:
        """
        Handles a status response.
        :param sender_identity: The ZMQ identity of the message sender.
        :param msg: The status response message.
        :return: None
        """
        sender_id_str = sender_identity.decode("utf-8")
        self._logger.debug(f"Received status from {sender_id_str}: {msg.status}")
        return None
