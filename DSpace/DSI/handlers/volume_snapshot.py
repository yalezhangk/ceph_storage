#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


@URLRegistry.register(r"/volume_snapshots/")
class VolumeSnapshotListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - volume_snapshot
        summary: volume snapshot List
        description: Return a list of volume snapshots
        operationId: volumesnapshots.api.listVolumeSnapshot
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
        expected_attrs = ['volume', 'pool', 'child_volumes']
        volume_snapshots = yield client.volume_snapshot_get_all(
            ctxt, expected_attrs=expected_attrs, **page_args)
        volume_snapshot_count = yield client.volume_snapshot_get_count(ctxt)
        self.write(objects.json_encode({
            "volume_snapshots": volume_snapshots,
            "total": volume_snapshot_count
        }))

    @gen.coroutine
    def post(self):
        """
        ---
        tags:
        - volume_snapshot
        summary: Create volume_snapshot
        description: Create volume_snapshot.
        operationId: volumesnapshots.api.createVolumeSnapshot
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: body
          name: volume_snapshot
          description: Created volume_snapshot object
          required: true
          schema:
            type: object
            properties:
              volume_snapshot:
                type: object
                description: volume_snapshot object
                properties:
                  volume_id:
                    type: integer
                    description: volume's id
                  display_name:
                    type: string
                    description: volume_snapshot's name
                  display_description:
                    type: string
                    description: description of volume snapshot
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        data = data.get("volume_snapshot")
        client = self.get_admin_client(ctxt)
        volume_snapshot = yield client.volume_snapshot_create(ctxt, data)
        self.write(objects.json_encode({
            "volume_snapshot": volume_snapshot
        }))


@URLRegistry.register(r"/volume_snapshots/([0-9]*)/")
class VolumeSnapshotHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, volume_snapshot_id):
        """
        ---
        tags:
        - volume_snapshot
        summary: Detail of the volume_snapshot
        description: Return detail infomation of volume_snapshot by id
        operationId: volumesnapshots.api.volumeSnapshotDetail
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
          description: Volume snapshot ID
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
        expected_attrs = ['volume', 'pool', 'child_volumes']
        volume_snapshot = yield client.volume_snapshot_get(
            ctxt, volume_snapshot_id, expected_attrs=expected_attrs)
        self.write(objects.json_encode({"volume_snapshot": volume_snapshot}))

    @gen.coroutine
    def put(self, volume_snapshot_id):
        """
        ---
        tags:
        - volume_snapshot
        summary: Update volume_snapshot
        description: update volume_snapshot.
        operationId: volumesnapshots.api.updateVolumeSnapshot
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
          description: Volume_snapshot ID
          schema:
            type: integer
            format: int32
          required: true
        - in: body
          name: volume_snapshot
          description: updated volume_snapshot object
          required: true
          schema:
            type: object
            properties:
              volume_snapshot:
                type: object
                properties:
                  display_name:
                    type: string
                    description: volume_snapshot's name
                  display_description:
                    type: string
                    description: description of volume snapshot
        responses:
        "200":
          description: successful operation
        """
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
        """
        ---
        tags:
        - volume_snapshot
        summary: Delete the volume_snapshot by id
        description: delete volume_snapshot by id
        operationId: volumesnapshots.api.deleteVolumeSnapshot
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
          description: Volume_snapshot's id
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
        volume_snapshot = yield client.volume_snapshot_delete(
            ctxt, volume_snapshot_id)
        self.write(objects.json_encode({
            "volume_snapshot": volume_snapshot
        }))


@URLRegistry.register(r"/volume_snapshots/([0-9]*)/action/")
class VolumeSnapshotActionHandler(ClusterAPIHandler):

    def _clone(self, client, ctxt, volume_snapshot_id, snapshot_data):
        return client.volume_create_from_snapshot(ctxt, volume_snapshot_id,
                                                  snapshot_data)

    @gen.coroutine
    def put(self, volume_snapshot_id):
        """
        ---
        tags:
        - volume_snapshot
        summary: Volume_snapshot clone
        description: clone volume_snapshot many times.
        operationId: volumesnapshots.api.volumeSnapshotClone
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
          description: Volume_snapshot ID
          schema:
            type: integer
            format: int32
          required: true
        - in: body
          name: volume_snapshot
          description: clone the volume_snapshot object
          required: true
          schema:
            type: object
            description: how to clone the volume_snapshot
            properties:
              action:
                type: string
                description: volume_snapshot's action, it can be clone
              volume_snapshot:
                type: object
                properties:
                  batch_create:
                    type: boolean
                    description: Do multiple operations at once or not
                  number:
                    type: integer
                    format: int32
                    description: How many times you want.
                  pool_id:
                    type: integer
                    format: int32
                    description: Pool ID
                  display_name:
                    type: string
                    description: volume_snapshot's name
                  display_description:
                    type: string
                    description: description of volume snapshot
                  is_link_clone:
                    type: boolean
                    description: lonk clone or not
        responses:
        "200":
          description: successful operation
        """
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
