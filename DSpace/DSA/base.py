#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time
from concurrent import futures

from DSpace import exception
from DSpace.common.config import CONF
from DSpace.context import RequestContext
from DSpace.DSM.client import AdminClientManager
from DSpace.tools.base import Executor
from DSpace.tools.base import SSHExecutor
from DSpace.utils import retry

logger = logging.getLogger(__name__)


class AgentBaseHandler(object):
    node = None
    ctxt = None
    admin = None

    def __init__(self, *args, **kwargs):
        super(AgentBaseHandler, self).__init__(*args, **kwargs)
        self.state = 'setup'
        self._executor = futures.ThreadPoolExecutor(
            max_workers=CONF.task_workers)
        self.ctxt = RequestContext(user_id="agent %s" % CONF.node_id,
                                   is_admin=False, cluster_id=CONF.cluster_id)
        self._get_node()

    def _wapper(self, fun, *args, **kwargs):
        try:
            fun(*args, **kwargs)
        except Exception as e:
            logger.exception("Unexpected exception: %s", e)

    def task_submit(self, fun, *args, **kwargs):
        self._executor.submit(self._wapper, fun, *args, **kwargs)

    def _get_node(self):
        endpoints = {
            "admin": {
                "ip": CONF.admin_ip,
                "port": CONF.admin_port,
            }
        }
        self.admin = AdminClientManager(
            self.ctxt, async_support=False, endpoints=endpoints
        ).get_client()
        retry_interval = 10
        retry_times = 0
        while retry_times < 5:
            try:
                self.node = self.admin.node_get(
                    self.ctxt, node_id=CONF.node_id)
                if self.node:
                    break
            except Exception as e:
                logger.error("Cannot connect to admin: %s", e)
                retry_times += 1
                time.sleep(retry_interval)
                retry_interval *= 2

    def _get_executor(self):
        return Executor()

    @retry(exception.IPConnectError)
    def _get_ssh_executor(self, node=None):
        if not node:
            node = self.node
        try:
            ssh_client = SSHExecutor(hostname=str(node.ip_address),
                                     password=node.password)
        except exception.StorException as e:
            logger.error("Connect to {} failed: {}".format(CONF.my_ip, e))
            raise exception.IPConnectError(ip=CONF.my_ip)
        return ssh_client
