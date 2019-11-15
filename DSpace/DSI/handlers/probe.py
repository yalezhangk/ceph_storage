#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class ProbeClusterNodesHandler(ClusterAPIHandler):

    @gen.coroutine
    def post(self):
        """
        ---
        tags:
        - probe
        summary: probe cluster nodes
        description: probe cluster nodes.
        operationId: probe.api.clusterNodes
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: body
          name: nodes
          description: Created lots of node object
          required: true
          schema:
            type: object
            properties:
              node:
                  type: object
                  properties:
                    ip:
                      type: string
                      description: node's ip
                    password:
                      type: string
                      description: node's password
                    user:
                      type: string
                      description: node's user
                    port:
                      type: string
                      description: node's port
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        logger.info("data(%s)", data)
        data = data.get('node')
        client = self.get_admin_client(ctxt)
        info = yield client.probe_cluster_nodes(ctxt, **data)
        logger.info("node(%s) probe(%s)", data, info)

        self.write(objects.json_encode({
            "info": info
        }))
