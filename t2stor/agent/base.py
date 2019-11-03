#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from concurrent import futures

from t2stor import exception
from t2stor.admin.client import AdminClientManager
from t2stor.common.config import CONF
from t2stor.context import RequestContext
from t2stor.tools.base import Executor
from t2stor.tools.base import SSHExecutor

logger = logging.getLogger(__name__)


class AgentBaseHandler(object):
    Node = None
    ctxt = None

    def __init__(self, *args, **kwargs):
        super(AgentBaseHandler, self).__init__(*args, **kwargs)
        self.executors = futures.ThreadPoolExecutor(max_workers=10)
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
