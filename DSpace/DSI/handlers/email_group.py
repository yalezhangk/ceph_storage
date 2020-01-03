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


email_group_schema = {
    "type": "object",
    "properties": {
        "email_group": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "minLength": 5,
                    "maxLength": 32
                },
                "emails": {
                    "type": "string",
                }},
            "required": ["name", "emails"],
        },
    },
    "required": ["email_group"],
}


@URLRegistry.register(r"/email_groups/")
class EmailGroupListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - email_group
        summary: email group List
        description: Return a list of email groups
        operationId: emailgroups.api.listEmailGroup
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
        expected_attrs = ['alert_groups']
        email_groups = yield client.email_group_get_all(
            ctxt, expected_attrs=expected_attrs, **page_args)
        email_group_count = yield client.email_group_get_count(ctxt)
        self.write(objects.json_encode({
            "email_groups": email_groups,
            "total": email_group_count
        }))

    @gen.coroutine
    def post(self):
        """
        ---
        tags:
        - email_group
        summary: Create email_group
        description: create email_group.
        operationId: emailgroups.api.createEmailGroup
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
          description: email_group ID
          schema:
            type: integer
            format: int32
          required: true
        - in: body
          name: email_group
          description: created email_group object
          required: true
          schema:
            type: object
            properties:
              email_group:
                type: object
                properties:
                  name:
                    type: string
                    description: email_group's name
                  emails:
                    type: string
                    description: a string of multiple email and joined ',',
                                 eg. "1042@qq.com,xxx@163.com,55@ss.com"
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=email_group_schema,
                 format_checker=draft7_format_checker)
        data = data.get("email_group")
        client = self.get_admin_client(ctxt)
        email_group = yield client.email_group_create(ctxt, data)
        self.write(objects.json_encode({
            "email_group": email_group
        }))


@URLRegistry.register(r"/email_groups/([0-9]*)/")
class EmailGroupHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, email_group_id):
        """
        ---
        tags:
        - email_group
        summary: Detail of the email group
        description: Return detail infomation of email group by id
        operationId: emailgroups.api.emailGroupDetail
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
          description: Email group ID
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
        email_group = yield client.email_group_get(ctxt, email_group_id,
                                                   expected_attrs)
        self.write(objects.json_encode({
            "email_group": email_group
        }))

    @gen.coroutine
    def put(self, email_group_id):
        """
        ---
        tags:
        - email_group
        summary: Update email_group
        description: update email_group.
        operationId: emailgroups.api.updateEmailGroup
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
          description: email_group ID
          schema:
            type: integer
            format: int32
          required: true
        - in: body
          name: email_group
          description: updated email_group object
          required: true
          schema:
            type: object
            properties:
              email_group:
                type: object
                properties:
                  name:
                    type: string
                    description: email_group's name
                  emails:
                    type: string
                    description: a string of multiple email and joined ',',
                                 eg. "1042@qq.com,xxx@163.com,55@ss.com"
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=email_group_schema,
                 format_checker=draft7_format_checker)
        email_group_data = data.get('email_group')
        client = self.get_admin_client(ctxt)
        email_group = yield client.email_group_update(
            ctxt, email_group_id, email_group_data)
        self.write(objects.json_encode({
            "email_group": email_group
        }))

    @gen.coroutine
    def delete(self, email_group_id):
        """
        ---
        tags:
        - email_group
        summary: Delete the email_group by id
        description: delete email_group by id
        operationId: emailgroups.api.deleteEmailGroup
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
          description: email_group's id
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
        email_group = yield client.email_group_delete(ctxt, email_group_id)
        self.write(objects.json_encode({
            "email_group": email_group
        }))
