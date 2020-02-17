from __future__ import print_function

import sys

from oslo_log import log as logging

from DSpace import objects
from DSpace import version
from DSpace.common.config import CONF
from DSpace.context import RequestContext
from DSpace.service import BaseClientManager
from DSpace.service import RPCClient

logger = logging.getLogger(__name__)


class WebSocketClient(RPCClient):

    def send_message(self, ctxt, obj, op_type, msg, resource_type=None):
        logger.info("websocket send message: op_type(%s) resource_type(%s)"
                    " msg(%s) obj(%s)", op_type, resource_type, msg, obj)
        response = self.call(ctxt, method="send_message", obj=obj,
                             op_type=op_type, msg=msg,
                             resource_type=resource_type)
        return response


class WebSocketClientManager(BaseClientManager):
    service_name = "websocket"
    client_cls = WebSocketClient
    clients = {}

    def get_client(self):
        ws_ip = self.context.ws_ip
        if ws_ip not in self.clients:
            endpoint = "{}:{}".format(ws_ip, CONF.websocket_port)
            logger.info("init ws endpoint: %s", endpoint)
            client = self.client_cls(endpoint,
                                     async_support=self.async_support)
            self.clients[ws_ip] = client
        return self.clients[ws_ip]


if __name__ == '__main__':
    CONF(sys.argv[1:], project='stor', version=version.version_string())
    logging.setup(CONF, "stor")
    objects.register_all()
    ctxt = RequestContext(user_id="xxx", project_id="stor", is_admin=False)
    client = WebSocketClientManager(ctxt).get_client()
    client.send_message(ctxt, None, "xxx", "fasdfffffffasdfasdfasd", "xxx")
