import http.server
import socketserver
import threading
from dataclasses import dataclass
from datetime import datetime

from daq.base import DAQJob
from daq.models import DAQJobConfig


@dataclass
class DAQJobServeHTTPConfig(DAQJobConfig):
    serve_path: str
    host: str
    port: int


class DAQJobServeHTTP(DAQJob):
    config_type = DAQJobServeHTTPConfig
    config: DAQJobServeHTTPConfig

    def __init__(self, config: DAQJobServeHTTPConfig):
        super().__init__(config)

    def start(self):
        # Start a BasicHTTPServer in this thread
        serve_path = self.config.serve_path

        class Handler(http.server.SimpleHTTPRequestHandler):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, directory=serve_path, **kwargs)

            def do_GET(self) -> None:
                self.path = self.path.replace(
                    "$TODAY", datetime.now().strftime("%Y-%m-%d")
                )
                return super().do_GET()

            def log_message(self, format: str, *args) -> None:
                pass

        def start_server():
            with socketserver.TCPServer(
                (self.config.host, self.config.port), Handler
            ) as httpd:
                self._logger.info(
                    f"Serving at port {self.config.host}:{self.config.port}"
                )
                httpd.serve_forever()

        thread = threading.Thread(target=start_server, daemon=True)
        thread.start()
        thread.join()
