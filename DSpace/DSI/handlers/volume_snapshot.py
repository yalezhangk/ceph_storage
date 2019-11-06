#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class VolumeSnapshotListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        page_args = self.get_paginated_args()
        expected_attrs = ['volume', 'pool', 'child_volumes']
        volume_snapshots = yield client.volume_snapshot_get_all(
            ctxt, expected_attrs=expected_attrs, **page_args)
        volume_snapshots_all = yield client.volume_snapshot_get_all(ctxt)
        self.write(objects.json_encode({
            "volume_snapshots": volume_snapshots,
            "total": len(volume_snapshots_all)
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
        expected_attrs = ['volume', 'pool', 'child_volumes']
        volume_snapshot = yield client.volume_snapshot_get(
            ctxt, volume_snapshot_id, expected_attrs=expected_attrs)
        self.write(objects.json_encode({"volume_snapshot": volume_snapshot}))

    @gen.coroutine
    def put(self, volume_snapshot_id):
        # 编辑:改名及描述
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        volume_data = data.get('volume_snapshot')
        client = self.get_admin_client(ctxt)
        volume_snapshot = yield client.volume_snapshot_update(
            ctxt, volume_snapshot_id, volume_data)
        self.write(objects.json_encode({
            "volume_snapshot": volume_snapshot
        }))

    @gen.coroutine
    def delete(self, volume_snapshot_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        volume_snapshot = yield client.volume_snapshot_delete(
            ctxt, volume_snapshot_id)
        self.write(objects.json_encode({
            "volume_snapshot": volume_snapshot
        }))


class VolumeSnapshotActionHandler(ClusterAPIHandler):

    def _clone(self, client, ctxt, volume_snapshot_id, snapshot_data):
        return client.volume_create_from_snapshot(ctxt, volume_snapshot_id,
                                                  snapshot_data)

    @gen.coroutine
    def put(self, volume_snapshot_id):
        # action:克隆
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        volume_snapshot_data = data.get('volume_snapshot')
        action = data.get('action')
        client = self.get_admin_client(ctxt)
        map_action = {
            'clone': self._clone
        }
        fun_action = map_action.get(action)
        if fun_action is None:
            raise exception.VolumeSnapshotActionNotFound(action=action)
        result = yield fun_action(client, ctxt, volume_snapshot_id,
                                  volume_snapshot_data)
        self.write(objects.json_encode({
            "volume": result
        }))
