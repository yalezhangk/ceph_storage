#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class EmailGroupListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        email_groups = yield client.email_group_get_all(ctxt)
        self.write(objects.json_encode({
            "email_groups": email_groups
        }))

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        data = data.get("email_group")
        client = self.get_admin_client(ctxt)
        email_group = yield client.email_group_create(ctxt, data)
        self.write(objects.json_encode({
            "email_group": email_group
        }))


class EmailGroupHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, email_group_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        email_group = yield client.email_group_get(ctxt, email_group_id)
        self.write(objects.json_encode({
            "email_group": email_group
        }))

    @gen.coroutine
    def put(self, email_group_id):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        email_group_data = data.get('email_group')
        client = self.get_admin_client(ctxt)
        email_group = yield client.email_group_update(
            ctxt, email_group_id, email_group_data)
        self.write(objects.json_encode({
            "email_group": email_group
        }))

    @gen.coroutine
    def delete(self, email_group_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        email_group = yield client.email_group_delete(ctxt, email_group_id)
        self.write(objects.json_encode({
            "email_group": email_group
        }))
