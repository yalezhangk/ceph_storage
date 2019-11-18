#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging

from netaddr import IPRange
from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler
from DSpace.i18n import _

logger = logging.getLogger(__name__)


@URLRegistry.register(r"/nodes/")
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
        """
        ---
        tags:
        - node
        summary: Create node
        description: Create node or nodes.
        operationId: nodes.api.createNode
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
          name: node
          description: Created node object
          required: true
          schema:
            type: object
            properties:
              node:
                type: object
                description: node object
                properties:
                  hostname:
                    type: string
                    description: node's hostname
                  ip_address:
                    type: string
                    description: node's ip address
                  password:
                    type: string
                    description: node's password, it can be null
                  gateway_ip_address:
                    type: string
                    description: node's gateway ip address
                  cluster_ip:
                    type: string
                    description: node's cluster ip
                  public_ip:
                    type: string
                    description: node's public ip
                  roles:
                    type: string
                    description: node's role, it can be
                                 monitor/storage/mds/radosgw/blockgw.
                                 And if you want more roles, you can
                                 use ',' to connect each role.
        - in: body
          name: nodes
          description: Created lots of node object
          required: true
          schema:
            type: object
            properties:
              nodes:
                type: array
                items:
                  type: object
                  properties:
                    hostname:
                      type: string
                      description: node's hostname
                    ip_address:
                      type: string
                      description: node's ip address
                    password:
                      type: string
                      description: node's password, it can be null
                    gateway_ip_address:
                      type: string
                      description: node's gateway ip address
                    cluster_ip:
                      type: string
                      description: node's cluster ip
                    public_ip:
                      type: string
                      description: node's public ip
                    roles:
                      type: string
                      description: node's role, it can be
                                   monitor/storage/mds/radosgw/blockgw.
                                   And if you want more roles, you can
                                   use ',' to connect each role.
        responses:
        "200":
          description: successful operation
        """
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


@URLRegistry.register(r"/nodes/([0-9]*)/")
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
        ---
        tags:
        - node
        summary: Update node
        description: update node's rack or hostname.
        operationId: nodes.api.updateNode
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
          description: node ID
          schema:
            type: integer
            format: int32
          required: true
        - in: body
          name: node(update rack)
          description: move node to another rack
          required: true
          schema:
            type: object
            properties:
              node:
                type: object
                properties:
                  rack:
                    type: integer
                    format: int32
                    description: node's rack id
        - in: body
          name: node(update name)
          description: updated node's name
          required: true
          schema:
            type: object
            properties:
              node:
                type: object
                properties:
                  name:
                    type: string
                    description: node's name
        responses:
        "200":
          description: successful operation
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


@URLRegistry.register(r"/nodes/([0-9]*)/role/")
class NodeRoleHandler(ClusterAPIHandler):
    @gen.coroutine
    def put(self, node_id):
        """
        ---
        tags:
        - node
        summary: Update node
        description: update node's role
        operationId: nodes.api.updateNodeRole
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
          description: node ID
          schema:
            type: integer
            format: int32
          required: true
        - in: body
          name: node
          description: updated node's role
          required: true
          schema:
            type: object
            properties:
              node:
                type: object
                properties:
                  install_roles:
                    type: array
                    items:
                      type: string
                      description: node's role, it can be
                                   monitor/storage/mds/radosgw/blockgw.
                  uninstall_roles:
                    type: array
                    items:
                      type: string
                      description: node's role, it can be
                                   monitor/storage/mds/radosgw/blockgw.
        responses:
        "200":
          description: successful operation
        """
        # monitor, mds, rgw, bgw role
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        node_data = data.get('node')
        client = self.get_admin_client(ctxt)
        node = yield client.node_roles_set(ctxt, node_id, node_data)

        self.write(objects.json_encode({
            "node": node
        }))


@URLRegistry.register(r"/nodes/([0-9]*)/metrics/")
class NodeMetricsMonitorHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, node_id):
        """
        ---
        tags:
        - node
        summary: node's Moniter Metrics
        description: return the Moniter Metrics of node by id
        operationId: nodes.api.getMoniterMetrics
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
          description: node's id
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
        data = yield client.node_metrics_monitor_get(ctxt, node_id=node_id)
        self.write(json.dumps({
            "node_metrics_monitor": data
        }))


@URLRegistry.register(r"/nodes/([0-9]*)/history_metrics/")
class NodeMetricsHistroyMonitorHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, node_id):
        """
        ---
        tags:
        - node
        summary: node's Monitor History Metrics
        description: return the Monitor History Metrics of node by id
        operationId: nodes.api.getMonitorHistoryMetrics
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
          description: node's id
          schema:
            type: integer
            format: int32
          required: true
        - in: request
          name: start
          description: the start of the history, it must be a time stamp.
                       eg.1573600118.935
          schema:
            type: integer
            format: int32
          required: true
        - in: request
          name: end
          description: the end of the history, it must be a time stamp.
                       eg.1573600118.936
          schema:
            type: integer
            format: int32
          required: true
        responses:
        "200":
          description: successful operation

        """
        ctxt = self.get_context()
        his_args = self.get_metrics_history_args()
        client = self.get_admin_client(ctxt)
        data = yield client.node_metrics_histroy_monitor_get(
            ctxt, node_id=node_id, start=his_args['start'], end=his_args[
                'end'])
        self.write(json.dumps({
            "node_metrics_histroy_monitor": data
        }))


@URLRegistry.register(r"/nodes/([0-9]*)/metrics/network/")
class NodeMetricsNetworkHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, node_id):
        """
        ---
        tags:
        - node
        summary: node's Network Metrics
        description: return the Network Metrics of node by id
        operationId: nodes.api.getNetworkMetrics
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
          description: node's id
          schema:
            type: integer
            format: int32
          required: true
        responses:
        "200":
          description: successful operation
        """
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


@URLRegistry.register(r"/nodes/([0-9]*)/history_network/")
class NodeMetricsHistroyNetworkHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, node_id):
        """
        ---
        tags:
        - node
        summary: node's Network History Metrics
        description: return the Network History Metrics of node by id
        operationId: nodes.api.getNetworkHistoryMetrics
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
          description: node's id
          schema:
            type: integer
            format: int32
          required: true
        - in: request
          name: start
          description: the start of the history, it must be a time stamp.
                       eg.1573600118.935
          schema:
            type: integer
            format: int32
          required: true
        - in: request
          name: end
          description: the end of the history, it must be a time stamp.
                       eg.1573600118.936
          schema:
            type: integer
            format: int32
          required: true
        responses:
        "200":
          description: successful operation

        """
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


@URLRegistry.register(r"/nodes/bare_node/")
class NodeListBareNodeHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - node
        summary: Returns a list of nodes that are not in the rack
        description: Returns a list of nodes that are not in the rack
        operationId: nodes.api.getBareNode
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        filters = {}
        filters['rack_id'] = None
        nodes = yield client.node_get_all(ctxt, filters=filters)
        self.write(objects.json_encode({
            "nodes": nodes
        }))


@URLRegistry.register(r"/nodes/get_infos/")
class NodeInfoHandler(ClusterAPIHandler):
    @gen.coroutine
    def post(self):
        """
        ---
        tags:
        - node
        summary: Get node's infomation
        description: Get node's infomation.
        operationId: nodes.api.getInfo
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
          name: node
          description: node's ip and password
          required: true
          schema:
            type: object
            properties:
              ips:
                type: array
                items:
                  type: object
                  properties:
                    ip_address:
                      type: string
                      description: node's ip address
                    password:
                      type: string
                      description: node's password
              ipr:
                type: array
                items:
                  type: string
                  description: nodes ip ranges
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        nodes_data = json_decode(self.request.body)
        logger.debug("node get info, param: %s", nodes_data)
        client = self.get_admin_client(ctxt)
        ips = nodes_data.get("ips")
        res = []
        for data in ips:
            info = yield client.node_get_infos(ctxt, data)
            res.append(info)

        ip_ranges = nodes_data.get("ipr")
        for ipr in ip_ranges:
            if '-' not in ipr:
                raise exception.InvalidInput(
                    reason=_("IP Range %s format error", ipr))
            start, end = ipr.split('-', 1)
            r = None
            try:
                r = IPRange(start, end)
            except Exception as e:
                logger.error(e)
                raise exception.InvalidInput(
                    reason=_("IP Range %s format error", ipr))
            for _ip in r:
                ip = str(_ip)
                info = yield client.node_get_infos(ctxt, {"ip_address": ip})
                res.append(info)

        self.write(objects.json_encode({
            "data": res
        }))


@URLRegistry.register(r"/nodes/check_node/")
class NodeCheckHandler(ClusterAPIHandler):
    @gen.coroutine
    def post(self):
        """
        ---
        tags:
        - node
        summary: Verify that the node can be installed
        description: Verify that the node can be installed.
        operationId: nodes.api.checkNode
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
          name: node
          description: node's information
          required: true
          schema:
            type: array
            items:
              type: object
              properties:
                admin_ip:
                  type: string
                  description: node's admin ip
                password:
                  type: string
                  description: node's password
                public_ip:
                  type: string
                  description: node's public ip
                cluster_ip:
                  type: string
                  description: node's cluster ip
        "200":
          description: successful operation
        """
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
