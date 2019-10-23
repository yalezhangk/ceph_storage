#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class DiskListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        page_args = self.get_paginated_args()

        filters = {}
        node_id = self.get_query_argument('node', default=None)
        if node_id:
            filters.update({
                'node_id': node_id
            })

        role = self.get_query_argument('role', default=None)
        if role:
            filters.update({
                'role': role
            })

        client = self.get_admin_client(ctxt)
        disks = yield client.disk_get_all(ctxt, filters=filters, **page_args)

        self.write(objects.json_encode({
            "disks": disks
        }))


class DiskHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        pass

    @gen.coroutine
    def post(self):
        pass
