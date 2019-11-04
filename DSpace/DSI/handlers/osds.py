#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class OsdListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """osd 列表
        """
        ctxt = self.get_context()
        page_args = self.get_paginated_args()
        client = self.get_admin_client(ctxt)
        expected_attrs = ['node', 'disk', 'db_partition', 'wal_partition',
                          'cache_partition', 'journal_partition']

        filters = {}
        supported_filters = ['node_id']
        for f in supported_filters:
            value = self.get_query_argument(f, default=None)
            if value:
                filters.update({
                    f: value
                })

        osds = yield client.osd_get_all(
            ctxt, filters=filters, expected_attrs=expected_attrs, **page_args)

        osd_count = yield client.osd_get_count(ctxt, filters=filters)
        self.write(objects.json_encode({
            "osds": osds,
            "total": osd_count
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
        expected_attrs = ['node', 'disk', 'db_partition', 'wal_partition',
                          'cache_partition', 'journal_partition']
        osd = yield client.osd_get(
            ctxt, osd_id, expected_attrs=expected_attrs)
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


class OsdMetricsHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, osd_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        data = yield client.osd_metrics_get(ctxt, osd_id=osd_id)
        self.write(json.dumps({
            "osd_metrics": data
        }))


class OsdMetricsHistoryHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, osd_id):
        ctxt = self.get_context()
        his_args = self.get_metrics_history_args()
        client = self.get_admin_client(ctxt)
        data = yield client.osd_metrics_history_get(
            ctxt, osd_id=osd_id, start=his_args['start'], end=his_args['end'])
        self.write(json.dumps({
            "osd_metrics_history": data
        }))