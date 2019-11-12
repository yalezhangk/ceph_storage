#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers.base import BaseAPIHandler
from DSpace.DSI.handlers.base import ClusterAPIHandler
from DSpace.objects.fields import AlertLogLevel

logger = logging.getLogger(__name__)


class AlertLogListHandler(ClusterAPIHandler):

    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        page_args = self.get_paginated_args()
        resource_type = self.get_query_argument('resource_type', default=None)
        readed = self.get_query_argument('readed', default=None)
        level = self.get_query_argument('level', default=None)
        filters = {}
        if resource_type:
            filters['resource_type'] = resource_type
        if readed:
            filters['readed'] = readed
        if level:
            filters['level'] = level
        alert_logs = yield client.alert_log_get_all(ctxt, filters=filters,
                                                    **page_args)
        alert_log_count = yield client.alert_log_get_count(ctxt,
                                                           filters=filters)
        self.write(objects.json_encode({
            "alert_logs": alert_logs,
            "total": alert_log_count
        }))


class AlertLogHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, alert_log_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        alert_log = yield client.alert_log_get(ctxt, alert_log_id)
        self.write(objects.json_encode({
            "alert_log": alert_log
        }))

    @gen.coroutine
    def put(self, alert_log_id):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        alert_log_data = data.get('alert_log')
        client = self.get_admin_client(ctxt)
        alert_log = yield client.alert_log_update(
            ctxt, alert_log_id, alert_log_data)
        self.write(objects.json_encode({
            "alert_log": alert_log
        }))

    @gen.coroutine
    def delete(self, alert_log_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        alert_log = yield client.alert_log_delete(ctxt, alert_log_id)
        self.write(objects.json_encode({
            "alert_log": alert_log
        }))


class ReceiveAlertMessageHandler(BaseAPIHandler):
    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        receive_datas = data.get("alerts")
        logger.debug('receive_datas:%s', receive_datas)
        client = self.get_admin_client(ctxt)
        alert_messages = yield client.send_alert_messages(ctxt, receive_datas)
        self.write(objects.json_encode({
            "alert_messages": alert_messages}))


class AlertTpyeCountHandler(ClusterAPIHandler):

    @gen.coroutine
    def get(self):
        """get alert_log level type and number

        ---
        tags:
        - alert_log
        summary:
        produces:
        - application/json
        parameters:
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        all_level = AlertLogLevel.ALL
        INFO = AlertLogLevel.INFO
        WARN = AlertLogLevel.WARN
        ERROR = AlertLogLevel.ERROR
        FATAL = AlertLogLevel.FATAL
        map_querys = {
            'unread': {'readed': False},
            INFO: {'level': INFO},
            WARN: {'level': WARN},
            ERROR: {'level': ERROR},
            FATAL: {'level': FATAL}
        }
        result_data = {}
        result_data.update({'level_type': all_level})
        for k, v in map_querys.items():
            count = yield client.alert_log_get_count(ctxt, filters=v)
            result_data.update({k: count})
        self.write(objects.json_encode({
            "alert_logs": result_data
        }))
