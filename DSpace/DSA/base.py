#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from concurrent import futures

from DSpace import exception
from DSpace.common.config import CONF
from DSpace.context import RequestContext
from DSpace.DSM.client import AdminClientManager
from DSpace.tools.base import Executor
from DSpace.tools.base import SSHExecutor

logger = logging.getLogger(__name__)


class AgentBaseHandler(object):
    Node = None
    ctxt = None

    def __init__(self, *args, **kwargs):
        super(AgentBaseHandler, self).__init__(*args, **kwargs)
        self.executor = futures.ThreadPoolExecutor(max_workers=10)
        self.ctxt = RequestContext(user_id="xxx", project_id="stor",
                                   is_admin=False, cluster_id=CONF.cluster_id)
        self._get_node()

    def _get_node(self):
        client = AdminClientManager(
            self.ctxt, CONF.cluster_id, async_support=False
        ).get_client()
        self.node = client.node_get(self.ctxt, node_id=CONF.node_id)

    def _get_executor(self):
        return Executor()

    def _get_ssh_executor(self, node):
        if not node:
            node = self.node
        try:
            ssh_client = SSHExecutor(hostname=node.hostname,
                                     password=node.password)
        except exception.StorException as e:
            logger.error("Connect to {} failed: {}".format(CONF.my_ip, e))
            return None
        return ssh_client
