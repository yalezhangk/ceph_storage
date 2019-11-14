#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.DSI.handlers.base import ClusterAPIHandler
from DSpace.i18n import _

logger = logging.getLogger(__name__)


class NodeListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - node
        summary: node List
        description: Return a list of nodes
        operationId: nodes.api.listNode
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: request
          name: limit
          description: Limit objects of response
          schema:
            type: integer
            format: int32
          required: false
        - in: request
          name: offset
          description: Skip objects of response
          schema:
            type: integer
            format: int32
          required: false
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        page_args = self.get_paginated_args()
        client = self.get_admin_client(ctxt)
        nodes = yield client.node_get_all(
            ctxt, expected_attrs=['disks', 'networks', 'osds'], **page_args)
        node_count = yield client.node_get_count(ctxt)
        self.write(objects.json_encode({
            "nodes": nodes,
            "total": node_count
        }))

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        if 'node' in data:
            data = data.get('node')
            client = self.get_admin_client(ctxt)
            node = yield client.node_create(ctxt, data)

            self.write(objects.json_encode({
                "node": node
            }))
        elif 'nodes' in data:
            datas = data.get('nodes')
            client = self.get_admin_client(ctxt)
            nodes = []
            for data in datas:
                try:
                    node = yield client.node_create(ctxt, data)
                    nodes.append(node)
                except Exception:
                    pass

            self.write(objects.json_encode({
                "nodes": nodes
            }))
        else:
            raise ValueError("data not accept: %s", data)


class NodeHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, node_id):
        """
        ---
        tags:
        - node
        summary: Detail of the node
        description: Return detail infomation of node by id
        operationId: nodes.api.nodeDetail
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: url
          name: id
          description: Node ID
          schema:
            type: integer
            format: int32
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        node = yield client.node_get(
            ctxt, node_id, expected_attrs=['disks', 'networks', 'osds'])
        self.write(objects.json_encode({
            "node": node
        }))

    @gen.coroutine
    def put(self, node_id):
        """修改节点信息
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        client = self.get_admin_client(ctxt)
        node = data.get("node")
        if "rack_id" in node:
            rack_id = node.get("rack_id")
            node = yield client.node_update_rack(ctxt, node_id, rack_id)
        else:
            node = yield client.node_update(ctxt, node_id, node)
        self.write(objects.json_encode({
            "node": node
        }))

    @gen.coroutine
    def delete(self, node_id):
        """
        ---
        tags:
        - node
        summary: Delete the node by id
        description: delete node by id
        operationId: nodes.api.deleteNode
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: url
          name: id
          description: Node's id
          schema:
            type: integer
            format: int32
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        node = yield client.node_delete(ctxt, node_id)
        self.write(objects.json_encode({
            "node": node
        }))


class NodeRoleHandler(ClusterAPIHandler):
    @gen.coroutine
    def put(self, node_id):
        # monitor, mds, rgw, bgw role
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        node_data = data.get('node')
        client = self.get_admin_client(ctxt)
        node = yield client.node_roles_set(ctxt, node_id, node_data)

        self.write(objects.json_encode({
            "node": node
        }))


class NodeMetricsMonitorHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, node_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        data = yield client.node_metrics_monitor_get(ctxt, node_id=node_id)
        self.write(json.dumps({
            "node_metrics_monitor": data
        }))


class NodeMetricsHistroyMonitorHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, node_id):
        ctxt = self.get_context()
        his_args = self.get_metrics_history_args()
        client = self.get_admin_client(ctxt)
        data = yield client.node_metrics_histroy_monitor_get(
            ctxt, node_id=node_id, start=his_args['start'], end=his_args[
                'end'])
        self.write(json.dumps({
            "node_metrics_histroy_monitor": data
        }))


class NodeMetricsNetworkHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, node_id):
        ctxt = self.get_context()
        net_name = self.get_query_argument('net_name', default=None)
        if not net_name:
            raise exception.InvalidInput(
                reason=_("node_metrics_network: net_name required"))
        client = self.get_admin_client(ctxt)
        data = yield client.node_metrics_network_get(ctxt, node_id=node_id,
                                                     net_name=net_name)
        self.write(json.dumps({
            "node_metrics_monitor": data
        }))


class NodeMetricsHistroyNetworkHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, node_id):
        ctxt = self.get_context()
        his_args = self.get_metrics_history_args()
        net_name = self.get_query_argument('net_name', default=None)
        if not net_name:
            raise exception.InvalidInput(
                reason=_("node_metrics_histroy_network: net_name required"))
        client = self.get_admin_client(ctxt)
        data = yield client.node_metrics_histroy_network_get(
            ctxt, node_id=node_id, net_name=net_name, start=his_args['start'],
            end=his_args['end'])
        self.write(json.dumps({
            "node_metrics_histroy_network_get": data
        }))


class NodeListBareNodeHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        filters = {}
        filters['rack_id'] = None
        nodes = yield client.node_get_all(ctxt, filters=filters)
        self.write(objects.json_encode({
            "nodes": nodes
        }))


class NodeInfoHandler(ClusterAPIHandler):
    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        nodes_data = json_decode(self.request.body)
        client = self.get_admin_client(ctxt)
        res = []
        for data in nodes_data:
            info = yield client.node_get_infos(ctxt, data)
            res.append(info)

        self.write(objects.json_encode({
            "data": res
        }))


class NodeCheckHandler(ClusterAPIHandler):
    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        nodes_data = json_decode(self.request.body)
        client = self.get_admin_client(ctxt)
        res = []
        for data in nodes_data:
            check_result = yield client.node_check(ctxt, data)
            res.append(check_result)

        self.write(objects.json_encode({
            "data": res
        }))
