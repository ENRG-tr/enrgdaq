import logging
import sys
from datetime import datetime

from enrgdaq.cnc.models import CNCMessageLog


class CNCLogHandler(logging.Handler):
    """
    A logging handler that sends log records to the CNC server.
    """

    def __init__(self, cnc_instance):
        super().__init__()
        self.cnc_instance = cnc_instance

    def emit(self, record):
        """
        Emit a log record by sending it to the CNC server.
        """
        try:
            # Format the log message
            # If no formatter is set, use the default formatting
            if self.formatter:
                log_message = self.format(record)
            else:
                # Use a simple default format if no formatter is set
                log_message = record.getMessage()
                if record.exc_info:
                    # Add exception info if present
                    import traceback

                    log_message += "\n" + "".join(
                        traceback.format_exception(*record.exc_info)
                    )

            # Create a CNC log message
            client_id = "unknown"
            if hasattr(self.cnc_instance, "supervisor_info") and hasattr(
                self.cnc_instance.supervisor_info, "supervisor_id"
            ):
                client_id = self.cnc_instance.supervisor_info.supervisor_id

            cnc_log_msg = CNCMessageLog(
                level=record.levelname,
                message=log_message,
                timestamp=datetime.now().isoformat(),
                module=record.name,
                client_id=client_id,
            )

            # Send the log message via the CNC system
            # For clients, we send the message directly to the server
            if not self.cnc_instance.is_server:
                self.cnc_instance._send_zmq_message(None, cnc_log_msg)

            # Propagate the log message to the root logger
            logging.getLogger().handle(record)

        except Exception as e:
            print(f"Error sending log to CNC server: {e}", file=sys.stderr)
