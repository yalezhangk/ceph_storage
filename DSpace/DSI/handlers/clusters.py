#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers.base import BaseAPIHandler
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class ClusterHandler(BaseAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        clusters = objects.ClusterList.get_all(ctxt)
        self.write(objects.json_encode({
            "clusters": clusters
        }))

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        cluster_data = data.get("cluster")

        client = self.get_admin_client(ctxt)

        cluster = yield client.cluster_create(ctxt, cluster_data)
        self.write(objects.json_encode({
            "cluster": cluster
        }))


class ClusterDetectHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        ip_address = self.get_argument('ip_address')
        password = self.get_argument('password')
        client = self.get_admin_client(ctxt)
        cluster_info = yield client.cluster_get_info(
            ctxt, ip_address, password=password)
        self.write(json.dumps(
            {"cluster_info": cluster_info}
        ))


class ClusterMetricsHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        data = yield client.cluster_metrics_get(ctxt)
        self.write(json.dumps({
            "cluster_metrics": data
        }))


class ClusterHistoryMetricsHandler(ClusterMetricsHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        his_args = self.get_metrics_history_args()
        client = self.get_admin_client(ctxt)
        data = yield client.cluster_history_metrics_get(
            ctxt, start=his_args['start'], end=his_args['end'])
        self.write(json.dumps({
            "cluster_history_metrics": data
        }))


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
