from __future__ import print_function

import sys

import six
from oslo_log import log as logging

from DSpace import objects
from DSpace import version
from DSpace.common.config import CONF
from DSpace.context import RequestContext
from DSpace.service import BaseClient
from DSpace.service.client import RPCMixin

logger = logging.getLogger(__name__)


class AgentClient(BaseClient):
    service_name = "agent"


class AgentClientManager(RPCMixin):
    _nodes = None
    _clients = None
    client_cls = AgentClient

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_inst'):
            cls._inst = super(AgentClientManager, cls).__new__(cls)
        return cls._inst

    def __init__(self, ctxt, port):
        logger.info("AgentClientManager init")
        self._port = port
        self._nodes = {}
        self._clients = {}
        self.ctxt = ctxt

    def _get_stub(self, node):
        return self.get_stub(node.ip_address, self._port)

    def add_node(self, node):
        logger.info("add node %s %s", node.id, node.hostname)
        if node.id in self._nodes:
            logger.warning("node %s already add", node.hostname)
        else:
            self._nodes[node.id] = node
        self.get_client(node.id)

    def del_node(self, node):
        logger.info("del node %s %s", node.id, node.hostname)
        if node.id not in self._nodes:
            logger.warning("node %s already deleted", node.hostname)
        self._nodes.pop(node.id, None)
        self._clients.pop(node.id, None)

    def get_client(self, node_id):
        if node_id not in self._nodes:
            logger.warning("node %s not add", node_id)
        if node_id not in self._clients:
            client = self.client_cls(self._get_stub(self._nodes[node_id]))
            self._clients[node_id] = client
        return self._clients[node_id]

    def is_alive(self, node):
        return True

    def ping(self):
        logger.info("AgentClientManager ping")
        for node_id, client in six.iteritems(self._clients):
            logger.info("ping node_id %s", node_id)
            client.check_dsa_status(ctxt)


if __name__ == '__main__':
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "stor")
    objects.register_all()
    ctxt = RequestContext(user_id="xxx", project_id="stor", is_admin=False)
    client = AgentClientManager(
        ctxt, cluster_id='7be530ce').get_client("devel")
    # print(client.disk_get_all(ctxt))
    print(client.service_restart(ctxt, "chronyd"))
