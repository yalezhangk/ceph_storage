#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import exception
from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler
from t2stor.i18n import _
from t2stor.objects import fields as s_fields

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

    @gen.coroutine
    def put(self, volume_id):
        # 编辑:改名
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        volume_data = data.get('volume')
        client = self.get_admin_client(ctxt)
        volume = yield client.volume_update(
            ctxt, volume_id, volume_data)
        self.write(objects.json_encode({
            "volume": volume
        }))

    @gen.coroutine
    def delete(self, volume_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        volume = yield client.volume_delete(ctxt, volume_id)
        self.write(objects.json_encode({
            "volume": volume
        }))


class VolumeActionHandler(ClusterAPIHandler):

    def _extend(self, client, ctxt, volume_id, volume_data):
        # todo check_size, can not over pool_size
        return client.volume_extend(ctxt, volume_id, volume_data)

    def _shrink(self, client, ctxt, volume_id, volume_data):
        # todo check_size, can not over pool_size
        return client.volume_shrink(ctxt, volume_id, volume_data)

    def _rollback(self, client, ctxt, volume_id, volume_data):
        snap_id = volume_data.get('volume_snapshot_id')
        snap = objects.VolumeSnapshot.get_by_id(ctxt, snap_id)
        if not snap:
            raise exception.VolumeSnapshotNotFound(volume_snapshot_id=snap_id)
        if snap.volume_id != int(volume_id):
            raise exception.InvalidInput(_(
                'The volume_id {} has not the snap_id {}').format(
                volume_id, snap_id))
        # todo other verify
        volume_data.update({'snap_name': snap.uuid})
        return client.volume_rollback(ctxt, volume_id, volume_data)

    def _unlink(self, client, ctxt, volume_id, volume_data=None):
        volume = objects.Volume.get_by_id(ctxt, volume_id)
        if not volume.is_link_clone:
            raise exception.Invalid(
                msg=_('the volume_name {} has not relate_snap').format(
                    volume.volume_name))
        return client.volume_unlink(ctxt, volume_id)

    @gen.coroutine
    def put(self, volume_id):
        # action:扩容、缩容、回滚、断开关系链
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        volume_data = data.get('volume')
        action = data.get('action')
        volume = objects.Volume.get_by_id(ctxt, volume_id)
        if not volume:
            raise exception.VolumeNotFound(volume_id=volume_id)
        if volume.status not in [s_fields.VolumeStatus.ACTIVE,
                                 s_fields.VolumeStatus.ERROR]:
            raise exception.VolumeStatusNotAllowAction()
        client = self.get_admin_client(ctxt)
        map_action = {
            'extend': self._extend,
            'shrink': self._shrink,
            'rollback': self._rollback,
            'unlink': self._unlink,
        }
        fun_action = map_action.get(action)
        if fun_action is None:
            raise exception.VolumeActionNotFound(action=action)
        result = yield fun_action(client, ctxt, volume_id, volume_data)
        self.write(objects.json_encode({
            "volume": result
        }))
