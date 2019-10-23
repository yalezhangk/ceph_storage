#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class AlertRuleListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        alert_rules = yield client.alert_rule_get_all(ctxt)
        self.write(objects.json_encode({
            "alert_rules": alert_rules
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
