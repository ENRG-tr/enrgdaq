from __future__ import annotations

import os
from typing import TYPE_CHECKING, Optional, Tuple

from enrgdaq.cnc.handlers.base import CNCMessageHandler
from enrgdaq.cnc.models import (
    CNCMessage,
    CNCMessageReqRestartDAQJobs,
    CNCMessageResRestartDAQJobs,
)

if TYPE_CHECKING:
    from enrgdaq.cnc.base import SupervisorCNC


class ReqRestartDAQJobsHandler(CNCMessageHandler):
    """
    Handler for CNCMessageReqRestartDAQJobs messages.
    """

    def __init__(self, cnc: SupervisorCNC):
        """
        Initialize the handler.
        :param cnc: The SupervisorCNC instance.
        """
        super().__init__(cnc)

    def handle(
        self, sender_identity: bytes, msg: CNCMessageReqRestartDAQJobs
    ) -> Optional[Tuple[CNCMessage, bool]]:
        """
        Handles a restart DAQJobs request.
        :param sender_identity: The ZMQ identity of the message sender.
        :param msg: The restart DAQJobs request message.
        :return: A restart DAQJobs response message.
        """
        self._logger.info("Received restart DAQJobs request.")

        try:
            # Find and terminate existing DAQJob processes
            import psutil

            killed_pids = []
            for proc in psutil.process_iter(["pid", "name", "cmdline"]):
                try:
                    # Look for processes that are specifically DAQJob processes
                    # We'll look for specific DAQJob indicators to avoid affecting other processes
                    cmdline = (
                        " ".join(proc.info["cmdline"]) if proc.info["cmdline"] else ""
                    )

                    # Look for processes that are specifically DAQJob-related
                    # This should be more specific than just "daq" and "python"
                    if (
                        ("daq_job" in cmdline.lower())
                        or ("enrgdaq" in cmdline.lower() and "daq" in cmdline.lower())
                    ) and "python" in cmdline.lower():
                        # Avoid terminating the current process or test processes
                        current_pid = os.getpid()
                        if proc.info["pid"] == current_pid:
                            continue

                        self._logger.info(
                            f"Terminating DAQJob process {proc.info['pid']}: {proc.info['name']}"
                        )
                        proc.terminate()
                        killed_pids.append(proc.info["pid"])
                except (
                    psutil.NoSuchProcess,
                    psutil.AccessDenied,
                    psutil.ZombieProcess,
                ):
                    pass

            # Wait for processes to terminate
            for pid in killed_pids:
                try:
                    p = psutil.Process(pid)
                    p.wait(timeout=5)  # Wait up to 5 seconds for graceful termination
                except psutil.TimeoutExpired:
                    self._logger.info(f"Force killing process {pid}")
                    try:
                        p.kill()  # Force kill if graceful termination takes too long
                    except psutil.NoSuchProcess:
                        pass  # Process already terminated

            success = True
            message = f"DAQJobs restarted. Terminated {len(killed_pids)} processes."
            self._logger.info(message)

        except Exception as e:
            success = False
            message = f"Error restarting DAQJobs: {str(e)}"
            self._logger.error(message)

        return CNCMessageResRestartDAQJobs(success=success, message=message), True
