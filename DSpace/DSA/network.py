#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from DSpace.DSA.base import AgentBaseHandler
from DSpace.tools.network import NetworkTool

logger = logging.getLogger(__name__)


class NetworkHandler(AgentBaseHandler):
    def network_get_all(self, ctxt, node):
        executor = self._get_executor()
        net_tool = NetworkTool(executor)
        networks = net_tool.all()
        networks.pop('lo', None)
        return networks
