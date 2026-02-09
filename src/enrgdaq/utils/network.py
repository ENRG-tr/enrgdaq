import socket


def get_available_port() -> int:
    """Find an available port by binding to port 0.

    The OS will assign an available port, which we then return.

    Returns:
        int: An available port number.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
