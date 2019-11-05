#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class AlertRuleListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        page_args = self.get_paginated_args()
        alert_rules = yield client.alert_rule_get_all(ctxt, **page_args)
        alert_rule_count = yield client.alert_rule_get_count(ctxt)
        self.write(objects.json_encode({
            "alert_rules": alert_rules,
            "total": alert_rule_count
        }))


class AlertRuleHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, alert_rule_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        rule = yield client.alert_rule_get(ctxt, alert_rule_id)
        self.write(objects.json_encode({
            "alert_rule": rule
        }))

    @gen.coroutine
    def put(self, alert_rule_id):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        alert_rule_data = data.get('alert_rule')
        client = self.get_admin_client(ctxt)
        rule = yield client.alert_rule_update(ctxt, alert_rule_id,
                                              alert_rule_data)

        self.write(objects.json_encode({
            "alert_rule": rule
        }))
