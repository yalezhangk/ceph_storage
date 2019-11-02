#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class ActionLogListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        action_logs = yield client.action_log_get_all(ctxt)
        self.write(objects.json_encode({
            "action_logs": action_logs
        }))

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        data = data.get("action_log")
        client = self.get_admin_client(ctxt)
        action_log = yield client.action_log_create(ctxt, data)
        self.write(objects.json_encode({
            "action_log": action_log
        }))


class ActionLogHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, action_log_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        action_log = yield client.action_log_get(ctxt, action_log_id)
        self.write(objects.json_encode({"action_log": action_log}))

    @gen.coroutine
    def put(self, action_log_id):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        action_log_data = data.get('action_log')
        client = self.get_admin_client(ctxt)
        action_log = yield client.action_log_update(
            ctxt, action_log_id, action_log_data)
        self.write(objects.json_encode({
            "action_log": action_log
        }))
