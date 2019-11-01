#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import exception
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

    @gen.coroutine
    def _extend(self, client, ctxt, volume_id, volume_data):
        # todo check_size, can not over pool_size
        extend = yield client.volume_extend(ctxt, volume_id, volume_data)
        return extend

    @gen.coroutine
    def _shrink(self, client, ctxt, volume_id, volume_data):
        # todo check_size, can not over pool_size
        shrink = yield client.volume_shrink(ctxt, volume_id, volume_data)
        return shrink

    @gen.coroutine
    def _rollback(self, client, ctxt, volume_id, volume_data):
        rollback = yield client.volume_rollback(ctxt, volume_id, volume_data)
        return rollback

    @gen.coroutine
    def _unlink(self, client, ctxt, volume_id, volume_data=None):
        unlink = yield client.volume_unlink(ctxt, volume_id)
        return unlink

    @gen.coroutine
    def put(self, volume_id):
        # action:扩容、缩容、回滚、断开关系链
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        volume_data = data.get('volume')
        action = data.get('action')
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
