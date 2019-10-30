#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import exception
from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler
from t2stor.i18n import _
from t2stor.utils import cluster_config

logger = logging.getLogger(__name__)


class CephConfigListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        page_args = self.get_paginated_args()

        filters = {}
        value = self.get_query_argument('group', default=None)
        if value:
            filters.update({
                'group': value
            })

        client = self.get_admin_client(ctxt)
        ceph_config = yield client.ceph_config_get_all(ctxt,
                                                       filters=filters,
                                                       **page_args)
        self.write(objects.json_encode({
            "ceph_configs": ceph_config
        }))


class CephConfigActionHandler(ClusterAPIHandler):
    def _ceph_config_set(self, ctxt, client, action, values):
        if action == 'update':
            required_args = ['group', 'key', 'value']
        if action == 'reset':
            required_args = ['group', 'key']

        for arg in required_args:
            if values.get(arg) is None:
                raise exception.InvalidInput(
                    reason=_("Ceph config: missing required arguments!"))

        detail = cluster_config.cluster_configs.get(values['key'])
        if not detail:
            raise exception.InvalidInput(
                reason=_("{} do not support to modify".format(values['key']))
            )
        if action == 'update':
            if not isinstance(values['value'], detail.get('type')):
                raise exception.InvalidInput(
                    reason=_("Type of config is error, it needs to be".format(
                        detail.get('type')))
                )
        if action == 'reset':
            values.update({'value': detail.get('default')})

        return client.ceph_config_set(ctxt, values=values)

    def _ceph_config_update(self, ctxt, client, values):
        return self._ceph_config_set(ctxt, client, 'update', values)

    def _ceph_config_reset(self, ctxt, client, values):
        return self._ceph_config_set(ctxt, client, 'reset', values)

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        body = json_decode(self.request.body)
        action = body.get('action')
        if not action:
            raise exception.InvalidInput(reason=_("Ceph config: action is "
                                                  "none"))
        values = body.get('ceph_config')
        if not values:
            raise exception.InvalidInput(reason=_("Ceph config: config is "
                                                  "none!"))
        client = self.get_admin_client(ctxt)
        action_map = {
            "config_update": self._ceph_config_update,
            "config_reset": self._ceph_config_reset,
        }
        fun_action = action_map.get(action)
        if fun_action is None:
            raise exception.DiskActionNotFound(action=action)
        config = yield fun_action(ctxt, client, values)

        self.write(objects.json_encode({
            "ceph_config": config
        }))


class CephConfigContentHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        content = yield client.ceph_config_content(ctxt)
        self.write(content)
