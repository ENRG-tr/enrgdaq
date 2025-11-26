from enrgdaq.cnc.handlers.base import BaseHandler
from enrgdaq.cnc.models import (
    CNCMessage,
    CNCMessageReqForward,
    CNCMessageResForward,
)


class ReqForwardHandler(BaseHandler):
    """Handles forwarding a message to another client and returning the response."""

    def handle(
        self, sender_identity: bytes, msg: CNCMessageReqForward
    ) -> tuple[CNCMessage, bool] | None:
        """
        Forwards a message to a specific client and waits for a response.
        """
        target_client_id = msg.client_id
        self._logger.info(
            f"Forwarding request to client '{target_client_id}' from '{sender_identity.decode()}'"
        )

        # Find the ZMQ identity of the target client
        client_info = self.cnc.clients.get(target_client_id)
        if not client_info:
            error_msg = f"Client '{target_client_id}' not found."
            self._logger.error(error_msg)
            response = CNCMessageResForward(
                client_id=target_client_id,
                payload=b"",
                success=False,
                error_message=error_msg,
            )
            return response, False

        target_identity = client_info["identity"]

        # Send the payload to the target client
        self.cnc.send_message(msg.payload, identity=target_identity, is_payload=True)

        # The response from the target client will be received by the server's main loop.
        # That response will be handled by a `Res*Handler` (e.g., ResPingHandler),
        # which needs to know to forward the response back to the original requester.
        # To do this, we'll store the original requester's identity.
        # The response handler will look up this stored identity to route the reply.
        self.cnc.pending_forwards[target_identity] = sender_identity

        # No direct response is sent here; the response is sent when the
        # forwarded client replies.
        return None
