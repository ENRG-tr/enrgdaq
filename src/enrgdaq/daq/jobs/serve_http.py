import http.server
import threading
from datetime import datetime, timedelta

from enrgdaq.daq.base import DAQJob
from enrgdaq.daq.models import DAQJobConfig

try:
    from socketserver import ForkingMixIn  # type: ignore
except ImportError:
    # For Windows
    from socketserver import ThreadingMixIn as ForkingMixIn


class DAQJobServeHTTPConfig(DAQJobConfig):
    """
    Configuration class for DAQJobServeHTTP.

    Attributes:
        serve_path (str): The path to serve files from.
        host (str): The host address to bind the server to.
        port (int): The port number to bind the server to.
    """

    serve_path: str
    host: str
    port: int


class DAQJobServeHTTP(DAQJob):
    """
    DAQ job to serve HTTP requests.

    Can be used to serve files from a specified path, primarily for CSV files.

    Handles placeholders in the path, such as "{TODAY}" and "{YESTERDAY}".

    Attributes:
        config_type (type): The configuration class type.
        config (DAQJobServeHTTPConfig): The configuration instance.
    """

    config_type = DAQJobServeHTTPConfig
    config: DAQJobServeHTTPConfig

    def start(self):
        # Start a BasicHTTPServer in this thread
        serve_path = self.config.serve_path

        class ThreadingHTTPServer(ForkingMixIn, http.server.HTTPServer):  # type: ignore
            pass

        class Handler(http.server.SimpleHTTPRequestHandler):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, directory=serve_path, **kwargs)

            def do_GET(self) -> None:
                """
                Handle GET requests and replace placeholders in the path.
                """
                REPLACE_DICT = {
                    "TODAY": datetime.now().strftime("%Y-%m-%d"),
                    "YESTERDAY": (datetime.now() - timedelta(days=1)).strftime(
                        "%Y-%m-%d"
                    ),
                }
                for key, value in REPLACE_DICT.items():
                    self.path = self.path.replace(key, value)

                return super().do_GET()

            def log_message(self, format: str, *args) -> None:
                """
                Override to suppress logging.
                """
                pass

        def start_server():
            """
            Start the HTTP server and serve requests indefinitely.
            """
            with ThreadingHTTPServer(
                (self.config.host, self.config.port), Handler
            ) as httpd:
                self._logger.info(
                    f"Serving at port {self.config.host}:{self.config.port}"
                )
                httpd.serve_forever()

        thread = threading.Thread(target=start_server, daemon=True)
        thread.start()
        thread.join()
