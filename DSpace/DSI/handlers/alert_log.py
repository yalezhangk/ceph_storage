#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from jsonschema import draft7_format_checker
from jsonschema import validate
from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import BaseAPIHandler
from DSpace.DSI.handlers.base import ClusterAPIHandler
from DSpace.i18n import _
from DSpace.objects.fields import AlertLogLevel as Level
from DSpace.objects.fields import AllResourceType as Resource

logger = logging.getLogger(__name__)


update_alert_log_schema = {
    "type": "object",
    "properties": {
        "alert_log": {
            "type": "object",
            "properties": {
                "readed": {"enum": [True]}
            },
            "required": ["readed"],
            "additionalProperties": False
        }
    },
    "required": ["alert_log"],
    "additionalProperties": False
}

update_alert_log_all_schema = {
    "type": "object",
    "properties": {
        "action": {
            "type": "string",
            "enum": ["all_readed", "del_alert_logs"]
        },
    },
    "allOf": [
        {
            "if": {
                "properties": {"action": {"const": "all_readed"}}
            },
            "then": {
                "properties": {"alert_log": {
                    "type": "object",
                    "properties": {
                        "readed": {"enum": [True]}
                    },
                    "required": ["readed"],
                    "additionalProperties": False
                }}
            }
        }, {
            "if": {
                "properties": {"action": {"const": "del_alert_logs"}}
            },
            "then": {
                "properties": {"alert_log": {
                    "type": "object",
                    "properties": {
                        "before_time": {
                            "type": "string",
                            "format": "date-time"
                        }
                    },
                    "required": ["before_time"],
                    "additionalProperties": False
                }}
            }
        },
    ], "required": ["action", "alert_log"]
}


@URLRegistry.register(r"/alert_logs/")
class AlertLogListHandler(ClusterAPIHandler):

    def _filter_query(self):
        resource_type = self.get_query_argument('resource_type', default=None)
        readed = self.get_query_argument('readed', default=None)
        level = self.get_query_argument('level', default=None)
        filters = {}
        if resource_type:
            if resource_type == 'all':
                pass
            else:
                if resource_type not in Resource.ALL:
                    raise exception.InvalidInput(_(
                        'resource_type:{} not exist').format(resource_type))

                filters['resource_type'] = resource_type
        if readed:
            if readed == 'all':
                pass
            else:
                filters['readed'] = False
        if level:
            if level == 'all':
                pass
            else:
                if level not in Level.ALL:
                    raise exception.InvalidInput(_(
                        'level:{} not exist').format(level))
                filters['level'] = level
        return filters

    @gen.coroutine
    def get(self):
        """alert_log list

        ---
        tags:
        - alert_log
        summary: get all alert_logs or filters get alert_logs
        produces:
        - application/json
        parameters:
        - in: URL
          name: resource_type
          description: all or one resource
          required: false
          schema:
            type: str
          name: level
          description: one or all alert_log level
          required: false
          schema:
            type: str
          name: readed
          description: all or unread alert_log
          required: false
          schema:
            type: str
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        page_args = self.get_paginated_args()
        filters = self._filter_query()
        expected_attrs = ['alert_rule']
        alert_logs = yield client.alert_log_get_all(
            ctxt, filters=filters, expected_attrs=expected_attrs, **page_args)
        alert_log_count = yield client.alert_log_get_count(ctxt,
                                                           filters=filters)
        self.write(objects.json_encode({
            "alert_logs": alert_logs,
            "total": alert_log_count
        }))


@URLRegistry.register(r"/alert_logs/([0-9]*)/")
class AlertLogHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, alert_log_id):
        """
        ---
        tags:
        - alert_log
        summary: Detail of the alert_log
        description: Return detail infomation of alert_log by id
        operationId: alertlogs.api.alertLogDetail
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
          description: Alert_log ID
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
        expected_attrs = ['alert_rule']
        alert_log = yield client.alert_log_get(
            ctxt, alert_log_id, expected_attrs)
        self.write(objects.json_encode({
            "alert_log": alert_log
        }))

    @gen.coroutine
    def put(self, alert_log_id):
        """
        ---
        tags:
        - alert_log
        summary: Update alert_log
        description: update alert_log.
        operationId: alertlogs.api.updateOneAlertLog
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
          description: Alert_log ID
          schema:
            type: integer
            format: int32
          required: true
        - in: body
          name: alert_log
          description: updated alert_log object
          required: true
          schema:
            type: object
            properties:
              alert_log:
                type: object
                properties:
                  readed:
                    type: boolean
                    description: It only can be true that
                                 means you had read it.
        responses:
        "200":
          description: successful operation

        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=update_alert_log_schema,
                 format_checker=draft7_format_checker)
        alert_log_data = data.get('alert_log')
        client = self.get_admin_client(ctxt)
        alert_log = yield client.alert_log_update(
            ctxt, alert_log_id, alert_log_data)
        self.write(objects.json_encode({
            "alert_log": alert_log
        }))

    @gen.coroutine
    def delete(self, alert_log_id):
        """
        ---
        tags:
        - alert_log
        summary: Delete the alert_log by id
        description: delete alert_log by id
        operationId: alertlogs.api.deleteAlertLog
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
          description: Alert_log's id
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
        alert_log = yield client.alert_log_delete(ctxt, alert_log_id)
        self.write(objects.json_encode({
            "alert_log": alert_log
        }))


@URLRegistry.register(r"/alert_logs/messages/")
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


@URLRegistry.register(r"/alert_logs/type_count/")
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
        all_level = Level.ALL
        INFO = Level.INFO
        WARN = Level.WARN
        ERROR = Level.ERROR
        FATAL = Level.FATAL
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


@URLRegistry.register(r"/alert_logs/action/")
class AlertLogActionHandler(ClusterAPIHandler):

    @gen.coroutine
    def _all_readed(self, client, ctxt, alert_log_data):
        result = yield client.alert_log_all_readed(ctxt, alert_log_data)
        return result

    @gen.coroutine
    def _del_alert_logs(self, client, ctxt, alert_log_data):
        result = yield client.alert_logs_set_deleted(ctxt, alert_log_data)
        return result

    @gen.coroutine
    def put(self):
        """alert_log action
        ---
        tags:
        - alert_log
        summary: Update alert_log, action:all_readed(全标已读)、
                 del_alert_logs(删除告警记录)
        description: update alert_log.
        operationId: alertlogs.api.updateAllAlertLog
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
          name: alert_logs(read)
          description: updated alert_logs object
          required: true
          schema:
            type: object
            properties:
              alert_log:
                type: object
                properties:
                  readed:
                    type: boolean
                    description: Update all of alert log to be read,
                                 it must be true.
              action:
                type: string
                description: Update all of alert log to be read,
                             it must be 'all_readed'.
        - in: body
          name: alert_logs(deleted)
          description: updated alert_logs object
          required: true
          schema:
            type: object
            properties:
              alert_log:
                type: object
                properties:
                  before_time:
                    type: string
                    description: times format, eg."2019-10-13 10:16:32".
              action:
                type: string
                description: Delete all of the alert log,
                             it must be 'del_alert_logs'.
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, update_alert_log_all_schema,
                 format_checker=draft7_format_checker)
        alert_log_data = data.get('alert_log')
        action = data.get('action')
        client = self.get_admin_client(ctxt)
        map_action = {
            'all_readed': self._all_readed,
            'del_alert_logs': self._del_alert_logs,
        }
        fun_action = map_action.get(action)
        if fun_action is None:
            raise exception.AlertLogActionNotFound(action=action)
        result = yield fun_action(client, ctxt, alert_log_data)
        self.write(objects.json_encode({
            "alert_log": result}))
