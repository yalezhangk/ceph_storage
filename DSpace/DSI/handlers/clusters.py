#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import BaseAPIHandler
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


@URLRegistry.register(r"/clusters/")
class ClusterHandler(BaseAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - cluster
        summary: return the cluster information
        description: return the clusters information
        operationId: clusters.api.getCluster
        produces:
        - application/json
        responses:
        "200":
          description: successful operation

        """
        ctxt = self.get_context()
        clusters = objects.ClusterList.get_all(ctxt)
        self.write(objects.json_encode({
            "clusters": clusters
        }))

    @gen.coroutine
    def post(self):
        """
        ---
        tags:
        - cluster
        summary: Create cluster
        description: Create cluster.
        operationId: clusters.api.createCluster
        produces:
        - application/json
        parameters:
        - in: body
          name: cluster
          description: Created cluster object
          required: true
          schema:
            type: object
            properties:
              cluster:
                type: object
                description: cluster object
                properties:
                  name:
                    type: string
                    description: cluster's name
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        cluster_data = data.get("cluster")

        client = self.get_admin_client(ctxt)

        cluster = yield client.cluster_create(ctxt, cluster_data)
        self.write(objects.json_encode({
            "cluster": cluster
        }))


@URLRegistry.register(r"/clusters/get_admin_nodes/")
class ClusterAdminNodesHandler(BaseAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - cluster
        summary: Collect admin nodes info
        description: Collect admin nodes info
        operationId: clusters.api.admin_nodeInfo
        produces:
        - application/json
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        admin_nodes = yield client.cluster_admin_nodes_get(ctxt)

        self.write(objects.json_encode({
            "admin_nodes": admin_nodes
        }))


@URLRegistry.register(r"/clusters/check_admin_node_status/")
class ClusterCheckAdminNodeHandler(BaseAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - cluster
        summary: Check admin node status
        description: Check admin node status
        operationId: clusters.api.checkAdminNode
        produces:
        - application/json
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        status = yield client.check_admin_node_status(ctxt)

        self.write(objects.json_encode({
            "status": status
        }))


@URLRegistry.register(r"/cluster_detect/")
class ClusterDetectHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - cluster
        summary: Detect an existing cluster
        description: Detect an existing cluster
        operationId: clusters.api.cetectCluster
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
          name: ip_address
          description: ip address where the cluster is.
          type: string
          required: true
        - in: request
          name: password
          description: the ip address' password
          type: string
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        ip_address = self.get_argument('ip_address')
        password = self.get_argument('password')
        client = self.get_admin_client(ctxt)
        cluster_info = yield client.cluster_get_info(
            ctxt, ip_address, password=password)
        self.write(json.dumps(
            {"cluster_info": cluster_info}
        ))


@URLRegistry.register(r"/clusters/metrics/")
class ClusterMetricsHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - cluster
        summary: Cluster's Metrics
        description: return the Metrics of cluster
        operationId: clusters.api.getMetrics
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
        data = yield client.cluster_metrics_get(ctxt)
        self.write(json.dumps({
            "cluster_metrics": data
        }))


@URLRegistry.register(r"/clusters/history_metrics/")
class ClusterHistoryMetricsHandler(ClusterMetricsHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - cluster
        summary: Cluster's History Metrics
        description: return the History Metrics of cluster
        operationId: clusters.api.getHistoryMetrics
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
        data = yield client.cluster_history_metrics_get(
            ctxt, start=his_args['start'], end=his_args['end'])
        self.write(json.dumps({
            "cluster_history_metrics": data
        }))


@URLRegistry.register(r"/clusters/services_status/")
class ClusterServiceStatus(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - cluster
        summary: Cluster service status overview
        description: Lists the total number of each state of services
        operationId: cluster.api.serviceStatus
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
        names = ["NODE_EXPORTER", "PROMETHEUS", "MON"]
        service_status = yield client.service_status_get(ctxt, names=names)
        self.write(json.dumps(service_status))


@URLRegistry.register(r"/clusters/host_status/")
class ClusterHostStatus(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - cluster
        summary: Cluster host status overview
        description: Lists the total number of each state of hosts
        operationId: cluster.api.hostStatus
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
        host_status = yield client.cluster_host_status_get(ctxt)
        self.write(json.dumps({"host_status": host_status}))


@URLRegistry.register(r"/clusters/pool_status/")
class ClusterPoolStatus(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - cluster
        summary: Cluster pool status overview
        description: Lists the total number of each state of pools
        operationId: cluster.api.poolStatus
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
        pool_status = yield client.cluster_pool_status_get(ctxt)
        self.write(json.dumps({"pool_status": pool_status}))


@URLRegistry.register(r"/clusters/osd_status/")
class ClusterOsdStatus(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - cluster
        summary: Cluster osd status overview
        description: Lists the total number of each state of osds
        operationId: cluster.api.osdStatus
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
        osd_status = yield client.cluster_osd_status_get(ctxt)
        self.write(json.dumps({"osd_status": osd_status}))
