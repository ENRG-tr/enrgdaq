from __future__ import annotations

import subprocess
import threading
from typing import TYPE_CHECKING, Optional, Tuple

from enrgdaq.cnc.handlers.base import CNCMessageHandler
from enrgdaq.cnc.models import (
    CNCMessage,
    CNCMessageReqRestartDAQ,
    CNCMessageResRestartDAQ,
)

if TYPE_CHECKING:
    from enrgdaq.cnc.base import SupervisorCNC

CNC_REQ_UPDATE_AND_RESTART_SECONDS = 2


class ReqRestartHandler(CNCMessageHandler):
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
        self, sender_identity: bytes, msg: CNCMessageReqRestartDAQ
    ) -> Optional[Tuple[CNCMessage, bool]]:
        """
        Handles an update and restart request.
        :param sender_identity: The ZMQ identity of the message sender.
        :param msg: The update and restart request message.
        :return: An update and restart response message.
        """
        self._logger.info("Received update and restart request.")

        try:
            if msg.update:
                # Run git pull
                git_result = subprocess.run(
                    ["git", "pull"], check=True, capture_output=True, text=True
                )
                self._logger.info(f"Git pull output: {git_result.stdout}")

                # Run uv sync
                sync_result = subprocess.run(
                    ["uv", "sync"], check=True, capture_output=True, text=True
                )
                self._logger.info(f"uv sync output: {sync_result.stdout}")

                message = f"Update completed successfully. Will terminate after {CNC_REQ_UPDATE_AND_RESTART_SECONDS} seconds."
            else:
                message = "Restart requested via CNC"
            success = True
            self._logger.info(message)

            # Schedule exit
            threading.Timer(CNC_REQ_UPDATE_AND_RESTART_SECONDS, self._exit).start()
        except subprocess.CalledProcessError as e:
            success = False
            message = f"Error during update: {str(e)}"
            self._logger.error(message)
        except Exception as e:
            success = False
            message = f"Unexpected error during update: {str(e)}"
            self._logger.error(message)

        return CNCMessageResRestartDAQ(success=success, message=message), True

    def _exit(self):
        self.cnc.supervisor.stop()
