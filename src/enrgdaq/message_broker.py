import logging
import threading
from typing import Dict, List

import zmq

logger = logging.getLogger(__name__)


class MessageBroker:
    """
    A message broker class that creates and manages XPUB and XSUB sockets for proxying messages.
    Uses ZMQ proxy functionality in separate threads.

    Attributes:
        context (zmq.Context): The ZMQ context.
        xpub_sockets (Dict[str, zmq.Socket]): A dictionary of XPUB sockets.
        xsub_sockets (Dict[str, zmq.Socket]): A dictionary of XSUB sockets.
        proxy_threads (Dict[str, threading.Thread]): A dictionary of proxy threads.
        running (bool): Flag indicating if the broker is running.
    """

    context: zmq.Context
    xpub_sockets: Dict[str, zmq.Socket]
    xsub_sockets: Dict[str, zmq.Socket]
    proxy_threads: Dict[str, threading.Thread]
    running: bool

    def __init__(self):
        self.context = zmq.Context()
        self.xpub_sockets = {}
        self.xsub_sockets = {}
        self.proxy_threads = {}
        self.running = False

    def add_xpub_socket(self, name: str, address: str) -> None:
        """
        Add a new XPUB socket to the broker.

        Args:
            name: Unique name for the socket
            address: ZMQ address (e.g., 'tcp://*:5555', 'ipc:///tmp/xpub.ipc')
        """
        if name in self.xpub_sockets:
            raise ValueError(f"XPUB socket with name '{name}' already exists")

        socket = self.context.socket(zmq.XPUB)
        socket.bind(address)
        self.xpub_sockets[name] = socket
        logger.info(f"Added XPUB socket '{name}' bound to {address}")

    def add_xsub_socket(self, name: str, address: str) -> None:
        """
        Add a new XSUB socket to the broker.

        Args:
            name: Unique name for the socket
            address: ZMQ address (e.g., 'tcp://*:5556', 'ipc:///tmp/xsub.ipc')
        """
        if name in self.xsub_sockets:
            raise ValueError(f"XSUB socket with name '{name}' already exists")

        socket = self.context.socket(zmq.XSUB)
        socket.bind(address)
        self.xsub_sockets[name] = socket
        logger.info(f"Added XSUB socket '{name}' bound to {address}")

    def start_proxy(self, name: str, xpub_name: str, xsub_name: str) -> None:
        """
        Start a ZMQ proxy thread between an XPUB and XSUB socket.

        Args:
            name: Unique name for the proxy thread
            xpub_name: Name of the XPUB socket to use
            xsub_name: Name of the XSUB socket to use
        """
        if name in self.proxy_threads:
            raise ValueError(f"Proxy with name '{name}' already exists")

        if xpub_name not in self.xpub_sockets:
            raise ValueError(f"No XPUB socket with name '{xpub_name}' exists")

        if xsub_name not in self.xsub_sockets:
            raise ValueError(f"No XSUB socket with name '{xsub_name}' exists")

        xpub_socket = self.xpub_sockets[xpub_name]
        xsub_socket = self.xsub_sockets[xsub_name]

        def proxy_function():
            try:
                logger.info(
                    f"Starting proxy '{name}' between {xpub_name} and {xsub_name}"
                )
                zmq.proxy(xpub_socket, xsub_socket)
            except zmq.ContextTerminated:
                logger.info(f"Proxy '{name}' terminated due to context termination")
            except Exception as e:
                logger.error(f"Error in proxy '{name}': {e}")
            finally:
                logger.info(f"Proxy '{name}' stopped")

        thread = threading.Thread(target=proxy_function, daemon=True)
        thread.start()
        self.proxy_threads[name] = thread
        logger.info(
            f"Started proxy thread '{name}' between {xpub_name} and {xsub_name}"
        )

    def stop(self) -> None:
        """
        Stop all proxy threads and close all sockets.
        """
        logger.info("Stopping message broker...")

        # Close all sockets
        for name, socket in self.xpub_sockets.items():
            logger.info(f"Closing XPUB socket '{name}'")
            socket.close()

        for name, socket in self.xsub_sockets.items():
            logger.info(f"Closing XSUB socket '{name}'")
            socket.close()

        # Terminate context
        logger.info("Terminating ZMQ context")
        self.context.term()

        logger.info("Message broker stopped")

    def get_socket_addresses(self) -> Dict[str, List[tuple]]:
        """
        Get all configured socket addresses for monitoring/debugging purposes.

        Returns:
            Dictionary mapping socket type to list of (name, address) tuples
        """
        addresses = {"xpub": [], "xsub": []}

        # For XPUB sockets, we can get the bound addresses
        for name, socket in self.xpub_sockets.items():
            try:
                addr_bytes = socket.getsockopt(zmq.LAST_ENDPOINT)
                if isinstance(addr_bytes, bytes):
                    addr = addr_bytes.decode("utf-8")
                else:
                    addr = str(addr_bytes)
                addresses["xpub"].append((name, addr))
            except zmq.Again:
                # Socket might not have an endpoint yet
                addresses["xpub"].append((name, "unbound"))

        # For XSUB sockets, we can get the bound addresses
        for name, socket in self.xsub_sockets.items():
            try:
                addr_bytes = socket.getsockopt(zmq.LAST_ENDPOINT)
                if isinstance(addr_bytes, bytes):
                    addr = addr_bytes.decode("utf-8")
                else:
                    addr = str(addr_bytes)
                addresses["xsub"].append((name, addr))
            except zmq.Again:
                # Socket might not have an endpoint yet
                addresses["xsub"].append((name, "unbound"))

        return addresses
