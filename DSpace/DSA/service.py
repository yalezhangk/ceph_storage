#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from DSpace.DSA.base import AgentBaseHandler
from DSpace.tools.docker import DockerSocket as DockerSockTool
from DSpace.tools.service import ServiceDbus as ServiceTool

logger = logging.getLogger(__name__)


class ServiceHandler(AgentBaseHandler):

    def docker_service_restart(self, ctxt, name):
        logger.debug("Restart container: %s", name)
        client = self._get_executor()
        docker_tool = DockerSockTool(client)
        docker_tool.restart(name)

    def docker_servcie_status(self, ctxt, name):
        logger.debug("Get container status: %s", name)
        client = self._get_executor()
        docker_tool = DockerSockTool(client)
        return docker_tool.status(name)

    def systemd_service_restart(self, ctxt, name):
        logger.debug("Restart systemd service: %s", name)
        if not name.endswith('.service'):
            name += '.service'
        client = self._get_executor()
        service_tool = ServiceTool(client)
        service_tool.reset_failed(name)
        service_tool.restart(name)

    def systemd_service_status(self, ctxt, name):
        logger.debug("Get systemd service status: %s", name)
        if not name.endswith('.service'):
            name += '.service'
        client = self._get_executor()
        service_tool = ServiceTool(client)
        return service_tool.status(name)
