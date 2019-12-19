#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging

from jsonschema import draft7_format_checker
from jsonschema import validate
from oslo_utils import strutils
from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import BaseAPIHandler
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


create_cluster_schema = {
    "type": "object",
    "properties": {
        "cluster": {
            "type": "object",
            "properties": {"cluster_name": {
                "type": "string",
                "minLength": 5,
                "maxLength": 32
            }}, "required": ["cluster_name"],
        },
    },
    "required": ["cluster"],
}


@URLRegistry.register(r"/clusters/")
class ClusterListHandler(BaseAPIHandler):
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
                  cluster_name:
                    type: string
                    description: cluster's name
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=create_cluster_schema,
                 format_checker=draft7_format_checker)
        cluster_data = data.get("cluster")

        client = self.get_admin_client(ctxt)

        cluster = yield client.cluster_create(ctxt, cluster_data)
        self.write(objects.json_encode({
            "cluster": cluster
        }))


@URLRegistry.register(r"/clusters/([0-9a-fA-F\-]{36})/")
class ClusterHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, cluster_id):
        """
        ---
        tags:
        - cluster
        summary: Detail of the cluster
        description: Return detail infomation of cluster by cluster_id
        operationId: clusters.api.clusterDetail
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
          description: cluster ID
          schema:
            type: string
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        cluster = yield client.cluster_get(ctxt, cluster_id)
        self.write(objects.json_encode({
            "cluster": cluster
        }))

    @gen.coroutine
    def delete(self, cluster_id):
        """
        ---
        tags:
        - cluster
        summary: Delete the cluster by cluster_id
        description: delete cluster by cluster_id
        operationId: clusters.api.deleteCluster
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
          description: Cluster's id
          schema:
            type: string
          required: true
        - in: request
          name: clean_ceph
          description: Clean ceph when delete cluster
          type: bool
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        clean_ceph = self.get_argument('clean_ceph')
        clean_ceph = True if clean_ceph == 'True' else False
        client = self.get_admin_client(ctxt)
        cluster = yield client.cluster_delete(
            ctxt, cluster_id, clean_ceph=clean_ceph)
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
        names = ["NODE_EXPORTER", "PROMETHEUS", "MON", "DSM", "DSI", "DSA",
                 "NGINX", "MARIADB", "ETCD", ]
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


@URLRegistry.register(r"/clusters/capacity_status/")
class ClusterCapacityStatus(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - cluster
        summary: Cluster capacity status overview
        description: Lists the capacity of the cluster or any pools
        operationId: cluster.api.clusterCapacity
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
        capacity_status = yield client.cluster_capacity_status_get(ctxt)
        self.write(json.dumps({"capacity_status": capacity_status}))


@URLRegistry.register(r"/clusters/pg_status/")
class ClusterPgStatus(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - cluster
        summary: Cluster pg status overview
        description: Lists the pg status of the cluster or any pools
        operationId: cluster.api.pgStatus
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
        pg_status = yield client.cluster_pg_status_get(ctxt)
        self.write(json.dumps({"pg_status": pg_status}))


@URLRegistry.register(r"/clusters/switch/")
class ClusterSwitch(ClusterAPIHandler):
    @gen.coroutine
    def put(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        data = json_decode(self.request.body)
        cluster_id = data.get('cluster_id')
        result = yield client.cluster_switch(ctxt, cluster_id)
        self.write(json.dumps({"cluster_id": result}))


@URLRegistry.register(r"/clusters/capacity/")
class ClusterCapacity(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - cluster
        summary: Cluster/Pools capacity overview
        description: Lists the capacity of the cluster or any pools by
                     prometheus
        operationId: cluster.api.clusterCapacity
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
          name: pool_id
          description: pool object ID
          schema:
            type: integer
            format: int32
          required: false
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        pool_id = self.get_argument('pool_id', default=None)
        capacity = yield client.cluster_capacity_get(ctxt, pool_id)
        self.write(json.dumps({"capacity": capacity}))


@URLRegistry.register(r"/clusters/data_balance/")
class ClusterDataBalance(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        data_balance = yield client.cluster_data_balance_get(ctxt)
        self.write(json.dumps({"data_balance": data_balance}))

    @gen.coroutine
    def post(self):
        """
        {"data_balance": {
            "action": on|off",
            "mode": "crush-compat|upmap"
        }}"""
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        data = json_decode(self.request.body)
        data_balance = data.get("data_balance")
        res = yield client.cluster_data_balance_set(ctxt, data_balance)
        self.write(json.dumps({"res": res}))


@URLRegistry.register(r"/clusters/pause/")
class ClusterPause(ClusterAPIHandler):
    @gen.coroutine
    def post(self):
        """
        ---
        tags:
        - cluster
        summary: cluster pause
        description: cluster pause.
        operationId: clusters.api.pause
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
          name: cluster
          description: Created cluster object
          required: true
          schema:
            type: object
            properties:
              pause:
                type: boolen
                description: cluster object
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        data = json_decode(self.request.body)
        pause = strutils.bool_from_string(data.get("pause"))
        res = yield client.cluster_pause(ctxt, pause)
        self.write(json.dumps({"res": res}))


@URLRegistry.register(r"/clusters/status/")
class ClusterStatus(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - cluster
        summary: cluster status
        description: cluster status.
        operationId: clusters.api.status
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
        res = yield client.cluster_status(ctxt)
        self.write(json.dumps({"cluster": res}))
