#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
import time

import six

from DSpace import exception
from DSpace.common.config import CONF
from DSpace.DSA.base import AgentBaseHandler
from DSpace.tools.docker import Docker as DockerTool
from DSpace.tools.service import Service as ServiceTool

logger = logging.getLogger(__name__)

container_roles = ["base", "role_admin"]
service_map = {
    "base": {
        "NODE_EXPORTER": "dspace_node_exporter",
        "CHRONY": "dspace_chrony",
        "DSA": "dsa",
    },
    "role_monitor": {
        "MON": "ceph-mon@$HOSTNAME",
        "MGR": "ceph-mgr@$HOSTNAME",
    },
    "role_admin": {
        "PROMETHEUS": "dspace_prometheus",
        "CONFD": "dspace_confd",
        "ETCD": "dspace_etcd",
        "PORTAL": "dspace_portal",
        "NGINX": "dspace_portal_nginx",
        "DSM": "dsm",
        "DSI": "dsi",
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
        super(CronHandler, self).__init__(*args, **kwargs)
        self.executor.submit(self._cron)
        self.executor.submit(self._setup)

    def _cron(self):
        logger.debug("Start crontab")
        crons = [
            self.service_check
        ]
        while True:
            for fun in crons:
                try:
                    fun()
                except Exception as e:
                    logger.exception("Cron Exception: %s", e)
            time.sleep(60)

    def _setup(self):
        logger.debug("Start setup")
        try:
            self.disks_reporter()
        except Exception as e:
            logger.exception("Setup Exception: %s", e)

    def service_check(self):
        logger.debug("Get services status")

        node = self.node
        ssh_client = self._get_ssh_executor()
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
        response = self.admin.service_update(self.ctxt, json.dumps(services))
        if not response:
            logger.debug('Update service status failed!')
            return False
        logger.debug('Update service status success!')
        return True

    def disks_reporter(self):
        disks = self.disk_get_all(self.ctxt, self.node)
        logger.info("Reporter disk info: %s", disks)
        self.admin.disk_reporter(self.ctxt, disks, self.node.id)
