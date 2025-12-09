from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Tuple

from enrgdaq.cnc.handlers.base import CNCMessageHandler
from enrgdaq.cnc.models import (
    CNCMessage,
    CNCMessageReqListClients,
    CNCMessageResListClients,
)

if TYPE_CHECKING:
    from enrgdaq.cnc.base import SupervisorCNC


class ReqListClientsHandler(CNCMessageHandler):
    """
    Handler for CNCMessageReqListClients messages.
    """

    def __init__(self, cnc: SupervisorCNC):
        """
        Initialize the handler.
        :param cnc: The SupervisorCNC instance.
        """
        super().__init__(cnc)

    def handle(
        self, sender_identity: bytes, msg: CNCMessageReqListClients
    ) -> Optional[Tuple[CNCMessage, bool]]:
        """
        Handles a list clients request.
        :param sender_identity: The ZMQ identity of the message sender.
        :param msg: The list clients request message.
        :return: A list clients response message.
        """
        sender_id_str = sender_identity.decode("utf-8")
        self._logger.debug(f"Received list clients request from {sender_id_str}")
        client_list = {cid: cinfo.info for cid, cinfo in self.cnc.clients.items()}
        return CNCMessageResListClients(clients=client_list), False
