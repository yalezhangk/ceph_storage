#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from jsonschema import draft7_format_checker
from jsonschema import validate
from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


update_alert_rule_schema = {
    "type": "object",
    "properties": {
        "alert_rule": {
            "type": "object",
            "properties": {"enabled": {"type": "boolean"}},
            "required": ["enabled"],
            "additionalProperties": False
        },
    },
    "required": ["alert_rule"],
    "additionalProperties": False
}


@URLRegistry.register(r"/alert_rules/")
class AlertRuleListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - alert_rule
        summary: alert_rule List
        description: Return a list of alert_rules
        operationId: alertules.api.listAlertRule
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: request
          name: limit
          description: Limit objects of response
          schema:
            type: integer
            format: int32
          required: false
        - in: request
          name: offset
          description: Skip objects of response
          schema:
            type: integer
            format: int32
          required: false
        - in: request
          name: resource_type
          description: Filters all of the alert rule by resource_type
          type: string
          required: false
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        page_args = self.get_paginated_args()
        resource_type = self.get_argument('resource_type', default=None)
        filters = {}
        if resource_type:
            filters = {'resource_type': resource_type}
        alert_rules = yield client.alert_rule_get_all(ctxt, filters=filters,
                                                      **page_args)
        alert_rule_count = yield client.alert_rule_get_count(
            ctxt, filters=filters)
        self.write(objects.json_encode({
            "alert_rules": alert_rules,
            "total": alert_rule_count
        }))


@URLRegistry.register(r"/alert_rules/([0-9]*)/")
class AlertRuleHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, alert_rule_id):
        """
        ---
        tags:
        - alert_rule
        summary: Detail of the alert_rule
        description: Return detail infomation of alert_rule by id
        operationId: alertrules.api.alertRuleDetail
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: url
          name: id
          description: Alert_rule ID
          schema:
            type: integer
            format: int32
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        expected_attrs = ['alert_groups']
        rule = yield client.alert_rule_get(ctxt, alert_rule_id, expected_attrs)
        self.write(objects.json_encode({
            "alert_rule": rule
        }))

    @gen.coroutine
    def put(self, alert_rule_id):
        """
        ---
        tags:
        - alert_rule
        summary: Update alert_rule
        description: update alert_rule.
        operationId: alertrules.api.updateAlertRule
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: url
          name: id
          description: Alert_rule ID
          schema:
            type: integer
            format: int32
          required: true
        - in: body
          name: alert_rule
          description: updated alert_rule object
          required: true
          schema:
            type: object
            properties:
              alert_rule:
                type: object
                properties:
                  enabled:
                    type: string
                    description: Enable alert rule or not
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=update_alert_rule_schema,
                 format_checker=draft7_format_checker)
        alert_rule_data = data.get('alert_rule')
        client = self.get_admin_client(ctxt)
        rule = yield client.alert_rule_update(ctxt, alert_rule_id,
                                              alert_rule_data)
        self.write(objects.json_encode({
            "alert_rule": rule
        }))
