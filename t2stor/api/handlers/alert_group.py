#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class AlertGroupListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        page_args = self.get_paginated_args()
        alert_groups_all = yield client.alert_group_get_all(ctxt)
        alert_groups = yield client.alert_group_get_all(ctxt, **page_args)
        self.write(objects.json_encode({
            "alert_groups": alert_groups,
            "total": len(alert_groups_all)
        }))

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        data = data.get("alert_group")
        client = self.get_admin_client(ctxt)
        alert_group = yield client.alert_group_create(ctxt, data)
        self.write(objects.json_encode({
            "alert_group": alert_group
        }))


class AlertGroupHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, alert_group_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        alert_group = yield client.alert_group_get(ctxt, alert_group_id)
        self.write(objects.json_encode({
            "alert_group": alert_group
        }))

    @gen.coroutine
    def put(self, email_group_id):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        alert_group_data = data.get('alert_group')
        client = self.get_admin_client(ctxt)
        alert_group = yield client.alert_group_update(
            ctxt, email_group_id, alert_group_data)
        self.write(objects.json_encode({
            "alert_group": alert_group
        }))

    @gen.coroutine
    def delete(self, alert_group_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        alert_group = yield client.alert_group_delete(ctxt, alert_group_id)
        self.write(objects.json_encode({
            "alert_group": alert_group
        }))
