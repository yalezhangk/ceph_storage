#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import exception
from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler
from t2stor.i18n import _

logger = logging.getLogger(__name__)


class DiskListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        page_args = self.get_paginated_args()

        filters = {}
        supported_filters = ['node', 'role', 'status']
        for f in supported_filters:
            value = self.get_query_argument(f, default=None)
            if value:
                filters.update({
                    f: value
                })

        client = self.get_admin_client(ctxt)
        disks = yield client.disk_get_all(ctxt, filters=filters, **page_args)

        self.write(objects.json_encode({
            "disks": disks
        }))


class DiskHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, disk_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        disk = yield client.disk_get(ctxt, disk_id)
        self.write(objects.json_encode({
            "disk": disk
        }))

    @gen.coroutine
    def put(self, disk_id):
        ctxt = self.get_context()
        disk = json_decode(self.request.body).get('disk')
        disk_type = disk.get('type')
        if not disk_type:
            raise exception.InvalidInput(reason=_("disk: disk type is none"))

        client = self.get_admin_client(ctxt)
        disk = yield client.disk_update(ctxt, disk_id, disk_type)
        logger.debug("Disk: id: {}, name: {}, cluster_id: {}".format(
            disk.id, disk.name, disk.cluster_id
        ))

        self.write(objects.json_encode({
            "disk": disk
        }))


class DiskActionHandler(ClusterAPIHandler):
    def _disk_light(self, ctxt, client, disk_id, values):
        return client.disk_light(ctxt, disk_id=disk_id, led=values['led'])

    def _disk_cache(self, ctxt, client, disk_id, values):
        required_args = ['partition_num', 'role']
        for arg in required_args:
            if values.get(arg) is None:
                raise exception.InvalidInput(
                    reason=_("disk: missing required arguments!"))

        return client.disk_cache(ctxt, disk_id=disk_id, values=values)

    def _disk_cache_create(self, ctxt, client, disk_id, values):
        return self._disk_cache(ctxt, client=client, disk_id=disk_id,
                                values=values)

    def _disk_cache_remove(self, ctxt, client, disk_id, values):
        return self._disk_cache(ctxt, client=client, disk_id=disk_id,
                                values=values)

    @gen.coroutine
    def post(self, disk_id):
        ctxt = self.get_context()
        body = json_decode(self.request.body)
        action = body.get('action')
        if not action:
            raise exception.InvalidInput(reason=_("disk: action is none"))
        disk = body.get('disk')

        client = self.get_admin_client(ctxt)
        action_map = {
            "light": self._disk_light,
            "cache_create": self._disk_cache_create,
            "cache_remove": self._disk_cache_remove,
        }
        fun_action = action_map.get(action)
        if fun_action is None:
            raise exception.DiskActionNotFound(action=action)
        disk = yield fun_action(ctxt, client, disk_id, disk)

        self.write(objects.json_encode({
            'disk': disk
        }))


class DiskSmartHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, disk_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        smart = yield client.disk_smart_get(ctxt, disk_id)
        self.write(objects.json_encode({
            "disk_smart": smart
        }))
