#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class OsdListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """osd 列表
        """
        ctxt = self.get_context()
        page_args = self.get_paginated_args()
        client = self.get_admin_client(ctxt)
        osds = yield client.osd_get_all(ctxt, **page_args)
        self.write(objects.json_encode({
            "osds": osds
        }))

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        data = data.get('osd')
        client = self.get_admin_client(ctxt)
        osd = yield client.osd_create(ctxt, data)

        self.write(objects.json_encode({
            "osd": osd
        }))


class OsdHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, osd_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        osd = yield client.osd_get(ctxt, osd_id)
        self.write(objects.json_encode({
            "osd": osd
        }))

    @gen.coroutine
    def put(self, osd_id):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        client = self.get_admin_client(ctxt)
        osd = data.get("osd")
        osd = yield client.osd_update(ctxt, osd_id, osd)
        self.write(objects.json_encode({
            "osd": osd
        }))

    @gen.coroutine
    def delete(self, osd_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        osd = yield client.osd_delete(ctxt, osd_id)
        self.write(objects.json_encode({
            "osd": osd
        }))
