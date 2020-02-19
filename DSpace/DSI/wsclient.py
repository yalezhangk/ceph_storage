from __future__ import print_function

import sys

from oslo_log import log as logging

from DSpace import objects
from DSpace import version
from DSpace.common.config import CONF
from DSpace.context import RequestContext
from DSpace.service import BaseClientManager
from DSpace.service import RPCClient
from DSpace.utils import no_exception

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
    _ws_ips = None

    def ws_ips(self, ctxt):
        if not self._ws_ips:
            admin_nodes = objects.NodeList.get_all(
                ctxt, filters={'role_admin': 1, 'cluster_id': '*'})
            self._ws_ips = [str(n.ip_address) for n in admin_nodes]
        return self._ws_ips

    def get_client(self, ws_ip):
        if ws_ip not in self.clients:
            endpoint = "{}:{}".format(ws_ip, CONF.websocket_port)
            logger.info("init ws endpoint: %s", endpoint)
            client = self.client_cls(endpoint,
                                     async_support=self.async_support)
            self.clients[ws_ip] = client
        return self.clients[ws_ip]

    @no_exception
    def send_message(self, ctxt, obj, op_type, msg, resource_type=None):
        ws_ip = ctxt.ws_ip
        if ws_ip:
            client = self.get_client(ws_ip)
            client.send_message(ctxt, obj, op_type, msg, resource_type)
            return
        for ws_ip in self.ws_ips(ctxt):
            client = self.get_client(ws_ip)
            client.send_message(ctxt, obj, op_type, msg, resource_type)


if __name__ == '__main__':
    CONF(sys.argv[1:], project='stor', version=version.version_string())
    logging.setup(CONF, "stor")
    objects.register_all()
    ctxt = RequestContext(user_id="xxx", project_id="stor", is_admin=False)
    client = WebSocketClientManager(ctxt).get_client()
    client.send_message(ctxt, None, "xxx", "fasdfffffffasdfasdfasd", "xxx")
