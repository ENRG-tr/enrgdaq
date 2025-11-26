from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING, Optional, Tuple

from enrgdaq.cnc.handlers.base import CNCMessageHandler
from enrgdaq.cnc.models import (
    CNCMessage,
    CNCMessageReqUpdateAndRestart,
    CNCMessageResUpdateAndRestart,
)

if TYPE_CHECKING:
    from enrgdaq.cnc.base import SupervisorCNC


class ReqUpdateAndRestartHandler(CNCMessageHandler):
    """
    Handler for CNCMessageReqUpdateAndRestart messages.
    """

    def __init__(self, cnc: SupervisorCNC):
        """
        Initialize the handler.
        :param cnc: The SupervisorCNC instance.
        """
        super().__init__(cnc)

    def handle(
        self, sender_identity: bytes, msg: CNCMessageReqUpdateAndRestart
    ) -> Optional[Tuple[CNCMessage, bool]]:
        """
        Handles an update and restart request.
        :param sender_identity: The ZMQ identity of the message sender.
        :param msg: The update and restart request message.
        :return: An update and restart response message.
        """
        self._logger.info("Received update and restart request.")

        try:
            # Run git pull
            git_result = subprocess.run(
                ["git", "pull"], check=True, capture_output=True, text=True
            )
            self._logger.info(f"Git pull output: {git_result.stdout}")

            # Run uv sync
            sync_result = subprocess.run(
                ["uv", "sync"], check=True, capture_output=True, text=True
            )
            self._logger.info(f"UV sync output: {sync_result.stdout}")

            success = True
            message = "Update completed successfully."

            self._logger.info(message)

        except subprocess.CalledProcessError as e:
            success = False
            message = f"Error during update: {str(e)}"
            self._logger.error(message)
        except Exception as e:
            success = False
            message = f"Unexpected error during update: {str(e)}"
            self._logger.error(message)

        return CNCMessageResUpdateAndRestart(success=success, message=message), True
