#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import json
from tornado import gen

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler


logger = logging.getLogger(__name__)


class NodeListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        self.write(json.dumps({}))


class NodeHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        self.write(json.dumps({}))

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        data = self.get_argument('data')
        client = self.get_admin_client(ctxt)
        ip_address = data.get('ip_address')
        password = data.get('password')

        node = objects.Node(
            ctxt, ip_address=ip_address, password=password,
            gateway_ip_address=data.get('gateway_ip_address'),
            storage_cluster_ip_address=data.get('storage_cluster_ip_address'),
            storage_public_ip_address=data.get('storage_public_ip_address'),
            status='deploying')

        yield client.cluster_install_agent(
            ctxt, ip_address=ip_address,
            password=password)

        self.write(json.dumps(
            {"id": node.id}
        ))
