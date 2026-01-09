"""
Supervisor Message Handler - Handles incoming messages from DAQJobs via ZMQ subscription.
"""

import logging
import pickle
import threading
from typing import Any, Callable

import zmq


class SupervisorMessageHandler:
    """
    Handles incoming messages from DAQJobs via ZMQ subscription.

    This class abstracts the ZMQ subscription logic, providing callbacks
    for different message types (stats reports, remote stats, etc.).
    """

    def __init__(
        self,
        xpub_url: str,
        supervisor_id: str,
        on_stats_report: Callable[[Any], None] | None = None,
        on_remote_stats: Callable[[Any], None] | None = None,
    ):
        """
        Initialize the message handler.

        Args:
            xpub_url: URL of the ZMQ XPUB socket to connect to.
            supervisor_id: ID of the supervisor (used for topic subscription).
            on_stats_report: Callback for DAQJobMessageStatsReport messages.
            on_remote_stats: Callback for DAQJobMessageStatsRemote messages.
        """
        self._xpub_url = xpub_url
        self._supervisor_id = supervisor_id
        self._on_stats_report = on_stats_report
        self._on_remote_stats = on_remote_stats

        self._logger = logging.getLogger(f"SupervisorMessageHandler({supervisor_id})")
        self._zmq_context: zmq.Context | None = None
        self._zmq_sub: zmq.Socket | None = None
        self._subscriber_thread: threading.Thread | None = None
        self._is_stopped = False

    @property
    def stats_topic(self) -> str:
        """Topic for stats messages. Uses stats.{supervisor_id} format."""
        return f"stats.{self._supervisor_id}"

    def start(self):
        """Start the subscriber thread to receive messages."""
        self._is_stopped = False
        self._zmq_context = zmq.Context()
        self._zmq_sub = self._zmq_context.socket(zmq.SUB)
        self._zmq_sub.connect(self._xpub_url)

        # Subscribe to stats topic
        self._zmq_sub.subscribe(self.stats_topic)
        self._logger.debug(f"Subscribed to topic: {self.stats_topic}")

        self._subscriber_thread = threading.Thread(
            target=self._subscriber_loop, daemon=True
        )
        self._subscriber_thread.start()
        self._logger.info("Message handler started")

    def stop(self):
        """Stop the subscriber thread and clean up ZMQ resources."""
        if self._is_stopped:
            return

        self._is_stopped = True

        # Close the socket first to unblock recv_multipart()
        if self._zmq_sub:
            self._zmq_sub.setsockopt(zmq.LINGER, 0)
            self._zmq_sub.close()
            self._zmq_sub = None

        if self._zmq_context:
            self._zmq_context.term()
            self._zmq_context = None

        self._logger.info("Message handler stopped")

    def _subscriber_loop(self):
        """Receive and dispatch messages from DAQJobs."""
        from enrgdaq.daq.jobs.remote import DAQJobMessageStatsRemote
        from enrgdaq.daq.models import DAQJobMessageStatsReport

        while not self._is_stopped:
            try:
                assert self._zmq_sub is not None
                parts = self._zmq_sub.recv_multipart()

                if len(parts) < 2:
                    continue

                # parts[0] = topic, parts[1] = header, parts[2:] = buffers
                header = parts[1]
                buffers = parts[2:] if len(parts) > 2 else []
                message = pickle.loads(header, buffers=buffers)

                self._logger.debug(f"Received message: {type(message).__name__}")

                # Dispatch based on message type
                if isinstance(message, DAQJobMessageStatsReport):
                    if self._on_stats_report:
                        self._on_stats_report(message)
                elif isinstance(message, DAQJobMessageStatsRemote):
                    if self._on_remote_stats:
                        self._on_remote_stats(message)

            except zmq.ContextTerminated:
                break
            except Exception as e:
                if not self._is_stopped:
                    self._logger.error(f"Error in subscriber loop: {e}", exc_info=True)
