#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class VolumeSnapshotListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        volume_snapshots = yield client.volume_snapshot_get_all(ctxt)
        self.write(objects.json_encode({
            "volume_snapshots": volume_snapshots
        }))

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        data = data.get("volume_snapshot")
        client = self.get_admin_client(ctxt)
        volume_snapshot = yield client.volume_snapshot_create(ctxt, data)
        self.write(objects.json_encode({
            "volume_snapshot": volume_snapshot
        }))


class VolumeSnapshotHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, volume_snapshot_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        volume_snapshot = yield client.volume_snapshot_get(
            ctxt, volume_snapshot_id)
        self.write(objects.json_encode({"volume_snapshot": volume_snapshot}))
