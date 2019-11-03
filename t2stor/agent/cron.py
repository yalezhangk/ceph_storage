#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
import time

import six

from t2stor import exception
from t2stor.agent.base import AgentBaseHandler
from t2stor.common.config import CONF
from t2stor.tools.docker import Docker as DockerTool
from t2stor.tools.service import Service as ServiceTool

logger = logging.getLogger(__name__)

container_roles = ["base", "role_admin"]
service_map = {
    "base": {
        "NODE_EXPORTER": "t2stor_node_exporter",
        "CHRONY": "t2stor_chrony",
    },
    "role_monitor": {
        "MON": "ceph-mon@$HOSTNAME",
        "MGR": "ceph-mgr@$HOSTNAME",
    },
    "role_admin": {
        "PROMETHEUS": "t2stor_prometheus",
        "CONFD": "t2stor_confd",
        "ETCD": "t2stor_etcd",
        "PORTAL": "t2stor_portal",
        "NGINX": "t2stor_portal_nginx",
        "ADMIN": "t2stor_admin",
        "API": "t2stor_api",
    },
    "role_storage": {},
    "role_block_gateway": {
        "TCMU": "tcmu",
    },
    "role_object_gateway": {
        "RGW": "ceph-radosgw@rgw.$HOSTNAME",
    },
    "role_file_gateway": {},
}


class CronHandler(AgentBaseHandler):
    def __init__(self, *args, **kwargs):
        self.executors.submit(self._cron)

    def _cron(self):
        logger.debug("Start crontab")
        crons = [
            self.service_check
        ]
        while True:
            for fun in crons:
                fun()
            time.sleep(60)

    def service_check(self):
        logger.debug("Get services status")

        node = self.node
        ssh_client = self._get_ssh_client()
        if not ssh_client:
            return False
        docker_tool = DockerTool(ssh_client)
        service_tool = ServiceTool(ssh_client)
        services = []

        for role, sers in six.iteritems(service_map):
            if (role != "base") and (not node[role]):
                continue
            for k, v in six.iteritems(sers):
                v = v.replace('$HOSTNAME', node.hostname)
                try:
                    if role in container_roles:
                        status = docker_tool.status(name=v)
                    else:
                        status = service_tool.status(name=v)
                except exception.StorException as e:
                    logger.error("Get service status error: {}".format(e))
                    status = 'inactive'
                services.append({
                    "name": k,
                    "status": status,
                    "node_id": CONF.node_id
                })
        logger.debug(services)
        response = self.node.service_update(self.ctxt, json.dumps(services))
        if not response:
            logger.debug('Update service status failed!')
            return False
        logger.debug('Update service status success!')
        return True
