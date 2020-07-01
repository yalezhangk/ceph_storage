#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
import time

import six

from DSpace import exception
from DSpace.common.config import CONF
from DSpace.DSA.base import AgentBaseHandler
from DSpace.exception import RPCConnectError
from DSpace.objects import fields as s_fields
from DSpace.tools.docker import DockerSocket as DockerTool
from DSpace.tools.service import ServiceDbus as ServiceTool
from DSpace.utils import retry
from DSpace.utils.service_map import ServiceMap

logger = logging.getLogger(__name__)


class CronHandler(AgentBaseHandler):
    container_prefix = "athena"

    def __init__(self, *args, **kwargs):
        super(CronHandler, self).__init__(*args, **kwargs)
        self.map_util = ServiceMap(self.container_prefix)
        self.service_map = {}
        self.container_roles = self.map_util.container_roles
        self._service_map_init()
        self._setup()
        logger.info("dsa ready")
        self.state = 'ready'
        self.task_submit(self._cron)

    def _cron(self):
        logger.debug("Start service check crontab")
        while True:
            try:
                self.service_check()
            except Exception as e:
                logger.exception("Cron Exception: %s", e)
            time.sleep(CONF.service_heartbeat_interval)

    def _setup(self):
        logger.debug("Start setup")
        try:
            self.disks_reporter()
            self.network_reporter()
            self.node_summary_reporter()
        except Exception as e:
            logger.exception("Setup Exception: %s", e)

    def _service_map_init(self):
        self.service_map = {
            "base": self.map_util.base,
            "role_block_gateway": {},
            "role_radosgw_router": {},
            "role_object_gateway": {},
            "role_file_gateway": {},
        }
        if self.node.role_admin:
            self.service_map.update({
                "role_admin": self.map_util.role_admin
            })
        if self.node.role_monitor:
            self.service_map.update({
                "role_monitor": self.map_util.role_monitor
            })
        if self.node.role_block_gateway:
            self.service_map.update({
                "role_block_gateway": self.map_util.role_block_gateway
            })
        # map other service from dsm
        uncertain_services = self.admin.service_infos_get(
            self.ctxt, self.node)
        for k, v in six.iteritems(uncertain_services):
            if k == "radosgws":
                for rgw in v:
                    if rgw.status == s_fields.RadosgwStatus.STOPPED:
                        continue
                    self.service_map['role_object_gateway'].update({
                        rgw.name:
                            "ceph-radosgw@rgw.{}.service".format(rgw.name)
                    })
            if k == "radosgw_routers":
                for service in v:
                    self.service_map['role_radosgw_router'].update({
                        "radosgw_" + service.name:
                            self.container_prefix + "_radosgw_" +
                            service.name
                    })
        logger.info("Init service map sucess: %s", self.service_map)

    def _status_map(self, status, role):
        if role in ["base", "role_admin", "role_monitor",
                    "role_block_gateway"]:
            return s_fields.ServiceStatus.ACTIVE if status \
                else s_fields.ServiceStatus.INACTIVE
        if role == "role_object_gateway":
            return s_fields.RadosgwStatus.ACTIVE if status \
                else s_fields.RadosgwStatus.INACTIVE
        if role == "role_radosgw_router":
            return s_fields.RouterServiceStatus.ACTIVE if status \
                else s_fields.RouterServiceStatus.INACTIVE

    def _get_systemd_status(self, service_tool, name):
        status = service_tool.status(name)
        if status == "active":
            return True
        else:
            return False

    def _get_docker_status(self, docker_tool, name):
        status = docker_tool.status(container_name=name)
        logger.info("Container %s status: %s", name, status)
        if status == "running":
            return True
        else:
            return False

    def service_check(self):
        logger.debug("Check service according to map: %s", self.service_map)
        client = self._get_executor()
        try:
            docker_tool = DockerTool(client)
        except exception.StorException as e:
            logger.warning(e)
            docker_tool = None
        try:
            service_tool = ServiceTool(client)
        except exception.StorException as e:
            logger.warning(e)
            service_tool = None

        services = {}
        for role, sers in six.iteritems(self.service_map):
            services.update({role: []})
            for k, v in six.iteritems(sers):
                if k.lower() in self.service_ignore.lower():
                    logging.debug("Skip to check service '%s'" % k)
                    continue
                if v.find('$HOSTNAME'):
                    if not self.node.hostname:
                        continue
                v = v.replace('$HOSTNAME', self.node.hostname)
                try:
                    status = None
                    if role in self.container_roles:
                        if docker_tool:
                            status = self._get_docker_status(docker_tool, v)
                    else:
                        if service_tool:
                            status = self._get_systemd_status(service_tool, v)
                except exception.StorException as e:
                    logger.error("Get service status error: {}".format(e))
                    status = s_fields.ServiceStatus.INACTIVE
                logger.debug("status: %s, name: %s", status, k)
                status = self._status_map(status, role)
                services[role].append({
                    "name": k,
                    "status": status,
                    "node_id": CONF.node_id,
                    "service_name": v
                })
        logger.debug(services)
        response = self.admin.service_update(
            self.ctxt, json.dumps(services), self.node.id)
        if not response:
            logger.debug('Update service status failed!')
            return False
        logger.debug('Update service status success!')
        return True

    @retry(RPCConnectError)
    def disks_reporter(self):
        disks = self.disk_get_all(self.ctxt, self.node)
        logger.info("Reporter disk info: %s", disks)
        self.admin.disk_reporter(self.ctxt, disks, self.node.id)

    @retry(RPCConnectError)
    def network_reporter(self):
        networks = self.network_get_all(self.ctxt, self.node)
        logger.info("Reporter network info: %s", networks)
        self.admin.network_reporter(self.ctxt, networks, self.node.id)

    @retry(RPCConnectError)
    def node_summary_reporter(self):
        node_summary = self.node_get_summary(self.ctxt, self.node)
        logger.info("Reporter node summary: %s", node_summary)
        self.admin.node_reporter(self.ctxt, node_summary, self.node.id)

    def node_update_infos(self, ctxt, node):
        logger.info("Node information update: %s", self.node)
        self.node = node
        self._service_map_init()

    def service_map_remove(self, ctxt, role, name):
        logger.info("Remove from service_map: %s, %s", role, name)
        del self.service_map[role][name]

    def service_map_add(self, ctxt, role, key, value):
        logger.info("Add to service_map: %s, %s, %s", role, key, value)
        self.service_map[role].update({key: value})
