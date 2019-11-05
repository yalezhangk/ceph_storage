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
        data = data.get('node')
        client = self.get_admin_client(ctxt)
        node = yield client.node_create(ctxt, data)

        self.write(objects.json_encode({
            "node": node
        }))


class NodeHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, node_id):
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
