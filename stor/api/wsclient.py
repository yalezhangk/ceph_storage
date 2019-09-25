from __future__ import print_function
import logging

from stor import objects
from stor.service import BaseClient
from stor.service import BaseClientManager
from stor.context import RequestContext


class WebSocketClient(BaseClient):

    def send_message(self, ctxt, message):
        response = self.call(ctxt, method="send_message", message=message)
        return response


class WebSocketClientManager(BaseClientManager):
    service_name = "websocket"
    client_cls = WebSocketClient


if __name__ == '__main__':
    logging.basicConfig(level="DEBUG")
    objects.register_all()
    client = WebSocketClientManager(cluster_id='default').get_client()
    ctxt = RequestContext(user_id="xxx", project_id="stor", is_admin=False)
    client.send_message(ctxt, "xxx")
