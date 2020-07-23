#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import time

from DSpace import exception
from DSpace.common.config import CONF
from DSpace.context import RequestContext
from DSpace.DSM.client import AdminClientManager
from DSpace.objects.fields import ConfigKey
from DSpace.tools.base import Executor
from DSpace.tools.base import SSHExecutor
from DSpace.utils import retry
from DSpace.utils.threadpool import TheadPoolMixin

logger = logging.getLogger(__name__)


class AgentBaseHandler(TheadPoolMixin):
    node = None
    ctxt = None
    admin = None

    def __init__(self, *args, **kwargs):
        super(AgentBaseHandler, self).__init__(*args, **kwargs)
        self.state = 'setup'
        self.ctxt = RequestContext(user_id="agent %s" % CONF.node_id,
                                   is_admin=False, cluster_id=CONF.cluster_id)
        self._get_node()
        dsm_sys_info = self.admin.get_sysinfo(self.ctxt, [
            ConfigKey.CONTAINER_PREFIX,
            ConfigKey.PACKAGE_IGNORE,
            ConfigKey.SERVICE_IGNORE
        ])
        self.container_prefix = dsm_sys_info[ConfigKey.CONTAINER_PREFIX]
        self.package_ignore = dsm_sys_info[ConfigKey.PACKAGE_IGNORE]
        CONF.package_ignore = self.package_ignore

        self.service_ignore = dsm_sys_info[ConfigKey.SERVICE_IGNORE] or ""
        self.service_ignore = self.service_ignore.lower().split(",")

    def _get_node(self):
        endpoints = {
            "admin": "%s:%s" % (CONF.admin_ip, CONF.admin_port)
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
                    if (self.node.cluster_id == CONF.cluster_id and
                            self.node.id == CONF.node_id and
                            str(self.node.ip_address) == CONF.my_ip):
                        break
                    else:
                        logger.error("Node info does not match, exit. "
                                     "Node cluster id - %s, id - %s, ip - %s",
                                     self.node.cluster_id, self.node.id,
                                     self.node.ip_address)
                        os._exit(1)
            except exception.NodeNotFound as e:
                logger.error(e)
                os._exit(1)
            except Exception as e:
                logger.exception(e)
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
