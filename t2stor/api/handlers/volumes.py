#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class VolumeListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        volumes = yield client.volume_get_all(ctxt)
        self.write(objects.json_encode({
            "volumes": volumes
        }))

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        data = data.get("volume")
        client = self.get_admin_client(ctxt)
        v = yield client.volume_create(ctxt, data)
        self.write(objects.json_encode({
            "volume": v
        }))


class VolumeHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, volume_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        volume = yield client.volume_get(ctxt, volume_id)
        self.write(objects.json_encode({"volume": volume}))
