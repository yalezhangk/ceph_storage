#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from DSpace.DSA.base import AgentBaseHandler
from DSpace.tools.node import NodeTool

logger = logging.getLogger(__name__)


class NodeHandler(AgentBaseHandler):
    def node_get_summary(self, ctxt, node):
        executor = self._get_executor()
        node_tool = NodeTool(executor)
        node_summary = node_tool.summary()
        return node_summary
