#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import json

from tornado import gen

from stor.api.handlers.base import RPCAPIHandler


logger = logging.getLogger(__name__)


class VolumeListHandler(RPCAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        volumes = yield client.volume_get_all(ctxt)
        self.write(json.dumps([
            {"id": vol.id, "name": vol.name} for vol in volumes
        ]))


class VolumeHandler(RPCAPIHandler):
    def get(self, volume_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        volume = yield client.volume_get(ctxt, volume_id)
        self.write(json.dumps({"id": volume.id, "name": volume.name}))
