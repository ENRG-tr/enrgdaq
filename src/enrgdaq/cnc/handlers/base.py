from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional, Tuple

from enrgdaq.cnc.models import CNCMessage

if TYPE_CHECKING:
    from enrgdaq.cnc.base import SupervisorCNC


class CNCMessageHandler(ABC):
    """
    Abstract base class for C&C message handlers.
    """

    def __init__(self, cnc: SupervisorCNC):
        """
        Initialize the handler.
        :param cnc: The SupervisorCNC instance.
        """
        self.cnc = cnc
        self._logger = cnc._logger

    @abstractmethod
    def handle(
        self, sender_identity: bytes, msg: CNCMessage
    ) -> Optional[Tuple[CNCMessage, bool]]:
        """
        Handles an incoming C&C message.
        :param sender_identity: The ZMQ identity of the message sender.
        :param msg: The C&C message to handle.
        :return: An optional tuple containing the response message and a boolean
                 indicating if it's a forward reply.
        """
        pass
