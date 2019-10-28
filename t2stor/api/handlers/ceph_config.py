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

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        values = json_decode(self.request.body).get('ceph_config')
        if not values:
            raise exception.InvalidInput(reason=_("Ceph config: config is "
                                                  "none!"))

        required_args = ['group', 'key', 'value']
        for arg in required_args:
            if values.get(arg) is None:
                raise exception.InvalidInput(
                    reason=_("Ceph config: missing required arguments!"))

        client = self.get_admin_client(ctxt)
        config = yield client.ceph_config_set(ctxt, values=values)
        self.write(objects.json_encode({
            "ceph_config": config
        }))
