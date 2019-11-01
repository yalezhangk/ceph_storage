#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class AlertLogListHandler(ClusterAPIHandler):

    def handle_receive_datas(self, client, ctxt, datas):
        to_datas = []
        for alert in datas:
            resource_name = None
            resource_id = None
            labels = alert.get('labels')
            alert_value = alert['annotations']['description']
            alert_name = labels['alertname']
            rule = client.alert_rule_get_all(
                ctxt, filters={'type': alert_name}).first()
            if not rule:
                continue
            if not rule.enabled:
                continue
            resource_type = rule.resource_type
            if resource_type == 'node':
                pass
                # hostname = labels['hostname']
                # TODO get node filter
            elif resource_type == 'osd':
                pass
                # osd_id = labels['osd_id']
                # TODO get osd filter
            elif resource_type == 'pool':
                pass
                # pool_id = labels['pool_id']
                # TODO get pool filter
            elif resource_type == 'cluster':
                pass
            else:
                continue
            per_alert_log = {
                'resource_type': resource_type,
                'resource_name': resource_name,
                'resource_id': resource_id,
                'alert_role_id': rule.id,
                'level': rule.level,
                'alert_value': alert_value
            }
            to_datas.append(per_alert_log)
        return to_datas

    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        page_args = self.get_paginated_args()
        alert_logs = yield client.alert_log_get_all(ctxt, **page_args)
        alert_logs_all = yield client.alert_log_get_all(ctxt)
        self.write(objects.json_encode({
            "alert_logs": alert_logs,
            "total": len(alert_logs_all)
        }))

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        receive_datas = data.get("alert_logs")
        client = self.get_admin_client(ctxt)
        # todo handle receive_data
        to_datas = self.handle_receive_datas(client, ctxt, receive_datas)
        result = []
        for per_data in to_datas:
            alert_log = yield client.alert_log_create(ctxt, per_data)
            result.append(alert_log)
        self.write(objects.json_encode({
            "alert_logs": result
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
