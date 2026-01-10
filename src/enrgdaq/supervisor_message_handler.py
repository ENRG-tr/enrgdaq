"""
Supervisor Message Handler - Handles incoming messages from DAQJobs via ZMQ subscription.
"""

import logging
import pickle
import threading
from typing import Callable

import zmq

from enrgdaq.daq.jobs.handle_stats import (
    DAQJobMessageCombinedRemoteStats,
    DAQJobMessageCombinedStats,
)
from enrgdaq.daq.topics import Topic


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
        on_stats_receive: Callable[[DAQJobMessageCombinedStats], None] | None = None,
        on_remote_stats_receive: Callable[[DAQJobMessageCombinedRemoteStats], None]
        | None = None,
    ):
        """
        Initialize the message handler.

        Args:
            xpub_url: URL of the ZMQ XPUB socket to connect to.
            supervisor_id: ID of the supervisor (used for topic subscription).
        """
        self._xpub_url = xpub_url
        self._supervisor_id = supervisor_id
        self._on_stats_receive = on_stats_receive
        self._on_remote_stats_receive = on_remote_stats_receive

        self._logger = logging.getLogger(f"SupervisorMessageHandler({supervisor_id})")
        self._zmq_context: zmq.Context | None = None
        self._zmq_sub: zmq.Socket | None = None
        self._subscriber_thread: threading.Thread | None = None
        self._is_stopped = False

    def start(self):
        """Start the subscriber thread to receive messages."""
        self._is_stopped = False
        self._zmq_context = zmq.Context()
        self._zmq_sub = self._zmq_context.socket(zmq.SUB)
        assert self._zmq_sub is not None
        self._zmq_sub.connect(self._xpub_url)

        topics_to_subscribe = [Topic.stats_combined(self._supervisor_id)]
        for topic in topics_to_subscribe:
            self._zmq_sub.subscribe(topic)

        self._logger.info(f"Subscribed to topics: {', '.join(topics_to_subscribe)}")

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

                # Dispatch based on message type
                if isinstance(message, DAQJobMessageCombinedStats):
                    if self._on_stats_receive:
                        self._on_stats_receive(message)
                elif isinstance(message, DAQJobMessageCombinedRemoteStats):
                    if self._on_remote_stats_receive:
                        self._on_remote_stats_receive(message)

            except zmq.ContextTerminated:
                break
            except Exception as e:
                if not self._is_stopped:
                    self._logger.error(f"Error in subscriber loop: {e}", exc_info=True)
