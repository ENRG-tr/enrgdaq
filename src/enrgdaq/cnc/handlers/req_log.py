from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Tuple

from enrgdaq.cnc.handlers.base import CNCMessageHandler
from enrgdaq.cnc.models import CNCMessage, CNCMessageLog

if TYPE_CHECKING:
    from enrgdaq.cnc.base import SupervisorCNC


class ReqLogHandler(CNCMessageHandler):
    """
    Handler for CNCMessageLog messages.
    """

    def __init__(self, cnc: SupervisorCNC):
        """
        Initialize the handler.
        :param cnc: The SupervisorCNC instance.
        """
        super().__init__(cnc)

    def handle(
        self, sender_identity: bytes, msg: CNCMessageLog
    ) -> Optional[Tuple[CNCMessage, bool]]:
        """
        Handles a log message from a client.
        :param sender_identity: The ZMQ identity of the message sender.
        :param msg: The log message.
        :return: None
        """
        self.cnc.add_client_log(sender_identity.decode("utf-8"), msg)
        return None
