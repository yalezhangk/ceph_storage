from __future__ import print_function

import logging

from DSpace import objects
from DSpace.context import RequestContext
from DSpace.service import BaseClient
from DSpace.service import BaseClientManager


class WebSocketClient(BaseClient):

    def send_message(self, ctxt, obj, op_type, msg, resource_type=None):
        response = self.call(ctxt, method="send_message", obj=obj,
                             op_type=op_type, msg=msg,
                             resource_type=resource_type)
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
