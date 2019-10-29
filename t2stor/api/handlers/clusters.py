#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import objects
from t2stor.api.handlers.alert_rule_data_init import init_alert_rule
from t2stor.api.handlers.base import BaseAPIHandler
from t2stor.api.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class ClusterHandler(BaseAPIHandler):
    def get(self):
        ctxt = self.get_context()
        clusters = objects.ClusterList.get_all(ctxt)
        self.write(objects.json_encode({
            "clusters": clusters
        }))

    def post(self):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        cluster_data = data.get("cluster")
        cluster = objects.Cluster(ctxt, display_name=cluster_data.get('name'))
        cluster.create()
        # init alert_rule
        init_alert_rule(ctxt, cluster.id)
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
