#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from jsonschema import validate
from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)

create_alert_group_schema = {
    "type": "object",
    "properties": {
        "alert_group": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "minLength": 5,
                    "maxLength": 32
                },
                "alert_rule_ids": {
                    "type": "array",
                    "items": {"type": "number"},
                    "uniqueItems": True
                },
                "email_group_ids": {
                    "type": "array",
                    "items": {"type": "number"},
                    "uniqueItems": True
                }
            },
            "required": ["name", "alert_rule_ids", "email_group_ids"],
            "additionalProperties": False
        }
    },
    "required": ["alert_group"],
    "additionalProperties": False
}

update_alert_group_schema = {
    "type": "object",
    "properties": {
        "alert_group": {
            "type": "object",
            "minProperties": 1,
            "maxProperties": 1,
            "properties": {
                "name": {
                    "type": "string",
                    "minLength": 5,
                    "maxLength": 32
                },
                "alert_rule_ids": {
                    "type": "array",
                    "items": {"type": "number"},
                    "uniqueItems": True
                },
                "email_group_ids": {
                    "type": "array",
                    "items": {"type": "number"},
                    "uniqueItems": True
                }
            },
            "additionalProperties": False
        }
    },
    "additionalProperties": False,
    "required": ["alert_group"]
}


@URLRegistry.register(r"/alert_groups/")
class AlertGroupListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - alert_group
        summary: alert_group List
        description: Return a list of alert_groups
        operationId: alertgroups.api.listAlertGroup
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
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        page_args = self.get_paginated_args()
        expected_attrs = ['alert_rules', 'email_groups']
        alert_groups = yield client.alert_group_get_all(
            ctxt, expected_attrs=expected_attrs, **page_args)
        alert_group_count = yield client.alert_group_get_count(ctxt)
        self.write(objects.json_encode({
            "alert_groups": alert_groups,
            "total": alert_group_count
        }))

    @gen.coroutine
    def post(self):
        """
        ---
        tags:
        - alert_group
        summary: Create alert_group
        description: Create alert_group.
        operationId: alertgroups.api.createAlertGroup
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: body
          name: alert_group
          description: Created alert_group object
          required: true
          schema:
            type: object
            properties:
              alert_group:
                type: object
                description: alert_group object
                properties:
                  name:
                    type: string
                    description: alert_group's name
                  alert_rule_ids:
                    type: array
                    items:
                      type: integer
                      format: int32
                      description: alert_rule's ID
                  email_group_ids:
                    type: array
                    items:
                      type: integer
                      format: int32
                      description: email_group's ID
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=create_alert_group_schema)
        data = data.get("alert_group")
        client = self.get_admin_client(ctxt)
        alert_group = yield client.alert_group_create(ctxt, data)
        self.write(objects.json_encode({
            "alert_group": alert_group
        }))


@URLRegistry.register(r"/alert_groups/([0-9]*)/")
class AlertGroupHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, alert_group_id):
        """
        ---
        tags:
        - alert_group
        summary: Detail of the alert_group
        description: Return detail infomation of alert_group by id
        operationId: alertgroups.api.alertGroupDetail
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
          description: alert_group ID
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
        expected_attrs = ['alert_rules', 'email_groups']
        alert_group = yield client.alert_group_get(ctxt, alert_group_id,
                                                   expected_attrs)
        self.write(objects.json_encode({
            "alert_group": alert_group
        }))

    @gen.coroutine
    def put(self, email_group_id):
        """
        ---
        tags:
        - alert_group
        summary: Update alert_group
        description: Update alert_group.
        operationId: alertgroups.api.updateAlertGroup
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: body
          name: alert_group(update name)
          description: updated alert_group object. One Choice from Three.
          required: true
          schema:
            type: object
            properties:
              alert_group:
                type: object
                description: alert_group object
                properties:
                  name:
                    type: string
                    description: update alert_group's name
        - in: body
          name: alert_group(update alert_rule)
          description: updated alert_group object. One Choice from Three.
          required: true
          schema:
            type: object
            properties:
              alert_group:
                type: object
                description: alert_group object
                properties:
                  alert_rule_ids:
                    type: array
                    items:
                      type: integer
                      format: int32
                      description: update alert_group's alert_rule
        - in: body
          name: alert_group(update email_group)
          description: updated alert_group object. One Choice from Three.
          required: true
          schema:
            type: object
            properties:
              alert_group:
                type: object
                description: alert_group object
                properties:
                  email_group_ids:
                    type: array
                    items:
                      type: integer
                      format: int32
                      description: update alert_group's email_group
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=update_alert_group_schema)
        alert_group_data = data.get('alert_group')
        client = self.get_admin_client(ctxt)
        alert_group = yield client.alert_group_update(
            ctxt, email_group_id, alert_group_data)
        self.write(objects.json_encode({
            "alert_group": alert_group
        }))

    @gen.coroutine
    def delete(self, alert_group_id):
        """
        ---
        tags:
        - alert_group
        summary: Delete the alert_group by id
        description: delete alert_group by id
        operationId: alertgroups.api.deleteAlertGroup
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
          description: Alert_group's id
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
        alert_group = yield client.alert_group_delete(ctxt, alert_group_id)
        self.write(objects.json_encode({
            "alert_group": alert_group
        }))
