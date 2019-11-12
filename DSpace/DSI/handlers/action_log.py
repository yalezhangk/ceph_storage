#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class ActionLogListHandler(ClusterAPIHandler):

    def filters_query(self):
        resource_type = self.get_argument('resource_type', default=None)
        action = self.get_argument('action', default=None)
        status = self.get_argument('status', default=None)
        times = self.get_argument('between_time', default=None)
        filters = {}
        if resource_type:
            filters['resource_type'] = resource_type
        if action:
            filters['action'] = action
        if status:
            filters['status'] = status
        if times:
            times = times.split(',')
            filters['begin_time'] = [times[0], times[1]]
        return filters

    @gen.coroutine
    def get(self):
        """List action_logs

        ---
        tags:
        - action_log
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
          name: resource_type
          description: resource_type
          schema:
            type: string
          required: false
        - in: request
          name: action
          description: action type
          schema:
            type: string
          required: false
        - in: request
          name: status
          description: status
          schema:
            type: string
          required: false
        - in: request
          name: between_time
          description: between_time, eg:2019-11-04 08:07:06,2019-11-11 08:07:06
          schema:
            type: string
          required: false
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
        filters = self.filters_query()
        page_args = self.get_paginated_args()
        action_logs = yield client.action_log_get_all(ctxt, filters=filters,
                                                      **page_args)
        action_log_count = yield client.action_log_get_count(ctxt)
        self.write(objects.json_encode({
            "action_logs": action_logs,
            "total": action_log_count
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


class ResourceActionHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        resource_action = yield client.resource_action(ctxt)
        self.write(objects.json_encode(
            {'resource_action': resource_action}
        ))
