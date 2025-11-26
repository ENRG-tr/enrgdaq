from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Optional, Tuple

from enrgdaq.cnc.handlers.base import CNCMessageHandler
from enrgdaq.cnc.models import CNCMessage, CNCMessageHeartbeat

if TYPE_CHECKING:
    from enrgdaq.cnc.base import SupervisorCNC


class HeartbeatHandler(CNCMessageHandler):
    """
    Handler for CNCMessageHeartbeat messages.
    """

    def __init__(self, cnc: SupervisorCNC):
        """
        Initialize the handler.
        :param cnc: The SupervisorCNC instance.
        """
        super().__init__(cnc)

    def handle(
        self, sender_identity: bytes, msg: CNCMessageHeartbeat
    ) -> Optional[Tuple[CNCMessage, bool]]:
        """
        Handles a heartbeat message.
        :param sender_identity: The ZMQ identity of the message sender.
        :param msg: The heartbeat message.
        :return: None
        """
        sender_id_str = sender_identity.decode("utf-8")
        self._logger.info(f"Received heartbeat from {sender_id_str}")
        self.cnc.clients[sender_id_str] = {
            "identity": sender_identity,
            "last_seen": datetime.now().isoformat(),
            "info": msg.supervisor_info,
        }
        return None
