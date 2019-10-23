#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class NodeListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        page_args = self.get_paginated_args()
        client = self.get_admin_client(ctxt)
        nodes = yield client.node_get_all(ctxt, **page_args)
        self.write(objects.json_encode({
            "nodes": nodes
        }))


class NodeHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, node_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        node = yield client.node_get(ctxt, node_id)
        self.write(objects.json_encode({
            "node": node
        }))

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        data = data.get('node')
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
