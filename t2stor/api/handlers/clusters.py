#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import json
from tornado import gen

from t2stor import objects
from t2stor.api.handlers.base import BaseAPIHandler
from t2stor.api.handlers.base import RPCAPIHandler


logger = logging.getLogger(__name__)


class ClusterHandler(BaseAPIHandler):
    def get(self):
        ctxt = self.get_context()
        clusters = objects.ClusterList.get_all(ctxt)
        self.write(json.dumps([
            {"id": vol.id} for vol in clusters
        ]))

    def post(self):
        ctxt = self.get_context()
        name = self.get_argument('name')
        cluster = objects.Cluster(ctxt, display_name=name)
        cluster.create()
        self.write(json.dumps(
            {"id": cluster.id, "name": cluster.name}
        ))

class ClusterDetectHandler(RPCAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        ip_address = self.get_argument('ip_address')
        password = self.get_argument('password')
        client = self.get_admin_client(ctxt)
        cluster_info = yield client.cluster_get_info(ctxt, ip_address, password=password)
        self.write(json.dumps(
            {"cluster_info": cluster_info}
        ))
