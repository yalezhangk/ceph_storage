#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from jsonschema import draft7_format_checker
from jsonschema import validate
from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


create_volume_schema = {
    "type": "object",
    "properties": {
        "volume": {
            "type": "object",
            "properties": {
                "display_name": {
                    "type": "string",
                    "minLength": 5,
                    "maxLength": 32
                },
                "display_description": {
                    "type": "string",
                    "minLength": 0,
                    "maxLength": 255
                },
                "pool_id": {"type": "integer"},
                "size": {"type": "integer"},
                "batch_create": {'type': "boolean"},
                "number": {'type': "integer"},
            },
            "required": ["display_name", "pool_id", "size"],
        },
    },
    "required": ["volume"],
}

update_volume_schema = {
    "type": "object",
    "properties": {
        "volume": {
            "type": "object",
            "properties": {
                "display_name": {
                    "type": "string",
                    "minLength": 5,
                    "maxLength": 32
                },
            },
            "required": ["display_name"],
        },
    },
    "required": ["volume"],
}


@URLRegistry.register(r"/volumes/")
class VolumeListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - volume
        summary: volume List
        description: Return a list of volumes
        operationId: volumes.api.listVolume
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: request
          name: limit
          description: Limit objects of response
          schema:
            type: integer
            format: int32
          required: false
        - in: request
          name: offset
          description: Skip objects of response
          schema:
            type: integer
            format: int32
          required: false
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        page_args = self.get_paginated_args()
        expected_attrs = ['snapshots', 'pool', 'volume_access_path',
                          'volume_client_groups', 'parent_snap',
                          'volume_clients']
        joined_load = self.get_query_argument('joined_load', default=None)
        if joined_load == '0':
            expected_attrs = []
        exact_filters = ['status', 'pool_id']
        fuzzy_filters = ['display_name']
        filters = self.get_support_filters(exact_filters, fuzzy_filters)

        volumes = yield client.volume_get_all(
            ctxt, expected_attrs=expected_attrs, filters=filters, **page_args)
        volume_count = yield client.volume_get_count(ctxt, filters=filters)
        self.write(objects.json_encode({
            "volumes": volumes,
            "total": volume_count
        }))

    @gen.coroutine
    def post(self):
        """Create volume

        ---
        tags:
        - volume
        summary: Create osd
        description: Create osd or osds.
        operationId: osds.api.createOsd
        produces:
        - application/json
        parameters:
        - in: body
          name: volume
          description: Created volume or volumes object
          required: true
          schema:
            type: object
            properties:
              batch_create:
                type: bool
                description: is or not batch_create
              number:
                type: integer
                description: batch_create number
              display_name:
                type: string
                description: display_name
              display_description:
                type: string
                description: display_description
              pool_id:
                type: integer
                format: int32
              size:
                type: integer
                format: int32
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=create_volume_schema,
                 format_checker=draft7_format_checker)
        data = data.get("volume")
        client = self.get_admin_client(ctxt)
        v = yield client.volume_create(ctxt, data)
        self.write(objects.json_encode({
            "volume": v
        }))


@URLRegistry.register(r"/volumes/([0-9]*)/")
class VolumeHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, volume_id):
        """
        ---
        tags:
        - volume
        summary: Detail of the volume
        description: Return detail infomation of volume by id
        operationId: volumegs.api.volumeDetail
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: url
          name: id
          description: Volume ID
          schema:
            type: integer
            format: int32
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        expected_attrs = ['snapshots', 'pool', 'volume_access_path',
                          'volume_client_groups', 'parent_snap',
                          'volume_clients']
        volume = yield client.volume_get(ctxt, volume_id,
                                         expected_attrs=expected_attrs)
        self.write(objects.json_encode({"volume": volume}))

    @gen.coroutine
    def put(self, volume_id):
        """Update volume

        ---
        tags:
        - volume
        summary: update volume
        description: volume rename
        produces:
        - application/json
        parameters:
        - in: body
          name: volume
          description: volume rename
          required: true
          schema:
            type: object
            properties:
              display_name:
                type: string
                description: new display_name
        responses:
        "200":
          description: successful operation
        """
        # 编辑:改名
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=update_volume_schema,
                 format_checker=draft7_format_checker)
        volume_data = data.get('volume')
        client = self.get_admin_client(ctxt)
        volume = yield client.volume_update(
            ctxt, volume_id, volume_data)
        self.write(objects.json_encode({
            "volume": volume
        }))

    @gen.coroutine
    def delete(self, volume_id):
        """Delete volume

        ---
        tags:
        - volume
        summary: delete volume
        produces:
        - application/json
        parameters:
        - in: URL
          name: id
          description: volume id
          required: true
          schema:
            type: int
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        volume = yield client.volume_delete(ctxt, volume_id)
        self.write(objects.json_encode({
            "volume": volume
        }))


@URLRegistry.register(r"/volumes/([0-9]*)/action/")
class VolumeActionHandler(ClusterAPIHandler):

    @gen.coroutine
    def _extend(self, client, ctxt, volume_id, volume_data):
        extend = yield client.volume_extend(ctxt, volume_id, volume_data)
        return extend

    @gen.coroutine
    def _shrink(self, client, ctxt, volume_id, volume_data):
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
        """volume action

        ---
        tags:
        - volume
        summary: action:extend(扩容)、shrink(缩容)、rollback(回滚)、
                 unlink(断开关系链)
        produces:
        - application/json
        parameters:
        - in: body
          name: action
          description: action type
          required: true
          schema:
            type: str
        responses:
        "200":
          description: successful operation
        """
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
