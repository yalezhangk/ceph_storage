#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
        nodes = yield client.node_get_all(
            ctxt, expected_attrs=['disks', 'networks'], **page_args)
        self.write(objects.json_encode({
            "nodes": nodes
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
        node = yield client.node_get(ctxt, node_id)
        self.write(objects.json_encode({
            "node": node
        }))

    @gen.coroutine
    def put(self, node_id):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        client = self.get_admin_client(ctxt)
        node = data.get("node")
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
