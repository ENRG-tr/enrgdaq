"""
Watchdog timer for detecting hung loops.

Provides a reusable watchdog mechanism that can be used by any component
to detect if a loop has stopped making progress.
"""

import logging
import os
import threading
from typing import Optional


class Watchdog:
    """
    A watchdog timer that detects hung loops.

    The watchdog should be reset at the beginning of each loop iteration.
    If the loop doesn't reset the watchdog within the timeout period,
    the watchdog triggers and either signals the loop to exit or forces
    the process to terminate.

    Usage:
        # For loops that can check is_triggered():
        watchdog = Watchdog(timeout_seconds=60.0, logger=self._logger)

        try:
            while not watchdog.is_triggered():
                watchdog.reset()  # Reset at start of each iteration

                # Do work...

        finally:
            watchdog.stop()

        # For loops with blocking calls that may hang:
        watchdog = Watchdog(timeout_seconds=60.0, force_exit=True, logger=self._logger)

    Args:
        timeout_seconds: Time in seconds before the watchdog triggers.
            Set to 0 or negative to disable the watchdog.
        force_exit: If True, call os._exit(1) on timeout to force process
            termination. Use this when the main thread may be blocked in
            a call that never returns. Default: False.
        logger: Optional logger for logging timeout events.
    """

    def __init__(
        self,
        timeout_seconds: float = 0.0,
        force_exit: bool = False,
        logger: Optional[logging.Logger] = None,
    ):
        self._timeout_seconds = timeout_seconds
        self._force_exit = force_exit
        self._logger = logger or logging.getLogger(__name__)
        self._timer: Optional[threading.Timer] = None
        self._triggered = threading.Event()

    @property
    def timeout_seconds(self) -> float:
        """Get the configured timeout in seconds."""
        return self._timeout_seconds

    @property
    def is_enabled(self) -> bool:
        """Check if the watchdog is enabled (timeout > 0)."""
        return self._timeout_seconds > 0

    @property
    def force_exit(self) -> bool:
        """Check if force_exit mode is enabled."""
        return self._force_exit

    def reset(self) -> None:
        """
        Reset the watchdog timer.

        Call this at the beginning of each loop iteration to prevent
        the watchdog from triggering.
        """
        self._cancel_timer()
        if self.is_enabled:
            self._timer = threading.Timer(self._timeout_seconds, self._on_timeout)
            self._timer.daemon = True
            self._timer.start()

    def stop(self) -> None:
        """
        Stop and clean up the watchdog.

        Call this when exiting the loop (e.g., in a finally block).
        """
        self._cancel_timer()

    def is_triggered(self) -> bool:
        """
        Check if the watchdog has been triggered.

        Use this as the loop exit condition.

        Returns:
            True if the watchdog has triggered and the loop should exit.
        """
        return self._triggered.is_set()

    def _cancel_timer(self) -> None:
        """Cancel the current timer if running."""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _on_timeout(self) -> None:
        """Called when the watchdog timer expires."""
        self._logger.error(
            f"Watchdog timeout! Loop has not completed in "
            f"{self._timeout_seconds} seconds. "
            f"{'Forcing exit...' if self._force_exit else 'Signaling exit...'}"
        )
        self._triggered.set()

        if self._force_exit:
            # Use os._exit() to terminate immediately without cleanup.
            # This is necessary when the main thread is blocked in a call
            # that will never return. The supervisor will restart the job.
            os._exit(1)
