import http.server
import threading
from datetime import datetime, timedelta

from daq.base import DAQJob
from daq.models import DAQJobConfig

try:
    from socketserver import ForkingMixIn  # type: ignore
except ImportError:
    # For Windows
    from socketserver import ThreadingMixIn as ForkingMixIn


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

        class ThreadingHTTPServer(ForkingMixIn, http.server.HTTPServer):  # type: ignore
            pass

        class Handler(http.server.SimpleHTTPRequestHandler):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, directory=serve_path, **kwargs)

            def do_GET(self) -> None:
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
                pass

        def start_server():
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
