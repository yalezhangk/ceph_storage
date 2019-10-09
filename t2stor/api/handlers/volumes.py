#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import json

from tornado import gen
from tornado.escape import json_decode
from tornado.escape import json_encode

from t2stor.api.handlers.base import ClusterAPIHandler


logger = logging.getLogger(__name__)


class VolumeListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        volumes = yield client.volume_get_all(ctxt)
        self.write(json.dumps({
            "volumes": [
                {
                    "id": vol.id,
                    "name": vol.name
                } for vol in volumes
            ]
        }))

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        data = data.get("volume")
        client = self.get_admin_client(ctxt)
        v = yield client.volume_create(ctxt, data)
        self.write(json_encode({
            "volume": {
                "id": v.id,
                "display_name": v.display_name,
                "size": v.size,
            }
        }))


class VolumeHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, volume_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        volume = yield client.volume_get(ctxt, volume_id)
        self.write(json.dumps({"volume": {
            "id": volume.id,
            "name": volume.name
        }}))
