#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler
from t2stor.exception import InvalidInput
from t2stor.i18n import _

logger = logging.getLogger(__name__)


class DiskListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        page_args = self.get_paginated_args()

        filters = {}
        node_id = self.get_query_argument('node', default=None)
        if node_id:
            filters.update({
                'node_id': node_id
            })

        role = self.get_query_argument('role', default=None)
        if role:
            filters.update({
                'role': role
            })

        client = self.get_admin_client(ctxt)
        disks = yield client.disk_get_all(ctxt, filters=filters, **page_args)

        self.write(objects.json_encode({
            "disks": disks
        }))


class DiskHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        pass

    @gen.coroutine
    def put(self, disk_id):
        ctxt = self.get_context()
        disk = json_decode(self.request.body).get('disk')
        disk_type = disk.get('type')
        if not disk_type:
            raise InvalidInput(reason=_("disk: disk type is none"))

        client = self.get_admin_client(ctxt)
        disk = yield client.disk_update(ctxt, disk_id, disk_type)
        logger.debug("Disk: id: {}, name: {}, cluster_id: {}".format(
            disk.id, disk.name, disk.cluster_id
        ))

        self.write(objects.json_encode({
            "disk": disk
        }))
