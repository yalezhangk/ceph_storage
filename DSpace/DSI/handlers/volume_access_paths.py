import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.DSI.handlers.base import ClusterAPIHandler
from DSpace.i18n import _

logger = logging.getLogger(__name__)


class CheckVolumeAccessPath():
    @gen.coroutine
    def check_volume_access_path_by_id(self, ctxt, client, id):
        v = yield client.volume_access_path_get(ctxt, id)
        if not v:
            raise exception.VolumeAccessPathNotFound(access_path_id=id)

    @gen.coroutine
    def check_volume_access_path_by_name(self, ctxt, client, name):
        filters = {"name": name}
        v = yield client.volume_access_path_get_all(ctxt, filters=filters)
        if v:
            raise exception.Duplicate(
                _("volume_access_path: {} is already exists!").format(name))


class VolumeAccessPathListHandler(ClusterAPIHandler, CheckVolumeAccessPath):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - volume_access_path
        summary: Volume Access Path List
        description: Return a list of Volume Access Path
        operationId: volume_access_paths.api.listVolumeAccessPath
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
        expected_attrs = ['volume_gateways', 'volume_client_groups',
                          'nodes', 'volumes', 'volume_clients']
        volume_access_paths = yield client.volume_access_path_get_all(
            ctxt, expected_attrs=expected_attrs, **page_args)
        vap_count = yield client.volume_access_path_get_count(ctxt)
        self.write(objects.json_encode({
            "volume_access_paths": volume_access_paths,
            "total": vap_count
        }))

    @gen.coroutine
    def post(self):
        """创建访问路径

        {"access_path":{"name":"iscsi-t1","type":"iscsi"}}

        ---
        tags:
        - volume_access_path
        summary: Create volume_access_path
        description: Create volume_access_path.
        operationId: volume_access_paths.api.createVolumeAccessPath
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
          name: access_path
          description: Created volume_access_path object
          required: true
          schema:
            type: object
            properties:
              access_path:
                type: object
                properties:
                  name:
                    type: string
                    description: volume_access_path's name
                  type:
                    type: string
                    description: volume_access_path's type, it can be iSCSI
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)

        # TODO 数据验证 传没传 -> 有没有
        data = json_decode(self.request.body).get('access_path')
        name = data.get("name")
        type = data.get("type")
        if not name or not type:
            raise exception.InvalidInput(_("name or type not found"))
        yield self.check_volume_access_path_by_name(ctxt, client, name)
        volume_access_path = yield client.volume_access_path_create(
            ctxt, data)
        self.write(objects.json_encode({
            "volume_access_path": volume_access_path
        }))


class VolumeAccessPathHandler(ClusterAPIHandler, CheckVolumeAccessPath):
    @gen.coroutine
    def get(self, voluem_access_path_id):
        """
        ---
        tags:
        - volume_access_path
        summary: Detail of the volume_access_path
        description: Return detail infomation of volume_access_path by id
        operationId: volume_access_path.api.volumeAccessPathDetail
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
          description: VolumeAccessPath's id
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
        expected_attrs = ['volume_gateways', 'volume_client_groups',
                          'nodes', 'volumes', 'volume_clients']
        volume_access_path = yield client.volume_access_path_get(
            ctxt, voluem_access_path_id, expected_attrs=expected_attrs)
        self.write(objects.json_encode({
            "volume_access_path": volume_access_path
        }))

    @gen.coroutine
    def put(self, id):
        """编辑访问路径

        {"access_path":{"name":"iscsi-t2"}

        ---
        tags:
        - volume_access_path
        summary: Update volume_access_path
        description: update volume_access_path.
        operationId: volume_access_paths.api.updateVolumeAccessPath
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
          description: VolumeAccessPath's id
          schema:
            type: integer
            format: int32
          required: true
        - in: body
          name: access_path
          description: updated volume_access_path object
          required: true
          schema:
            type: object
            properties:
              access_path:
                type: object
                properties:
                  name:
                    type: string
                    description: volume_access_path's name
        responses:
        "200":
          description: successful operation

        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)

        yield self.check_volume_access_path_by_id(ctxt, client, id)

        data = json_decode(self.request.body).get('access_path')
        if not data.get('name'):
            raise exception.InvalidInput(_("name or type not found"))
        yield self.check_volume_access_path_by_name(
            ctxt, client, data.get('name'))

        volume_access_path = yield client.volume_access_path_update(
            ctxt, id, data)
        self.write(objects.json_encode({
            "volume_access_path": volume_access_path
        }))

    @gen.coroutine
    def delete(self, id):
        """删除访问路径
        ---
        tags:
        - volume_access_path
        summary: Delete the volume_access_path by id
        description: delete volume_access_path by id
        operationId: volume_access_path.api.deleteVolumeAccessPath
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
          description: VolumeAccessPath's id
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
        yield self.check_volume_access_path_by_id(ctxt, client, id)
        # TODO 如果存在客户端组，不能删除
        # TODO 如果存在块存储网关，不能删除
        volume_access_path = yield client.volume_access_path_delete(ctxt, id)
        self.write(objects.json_encode({
            "volume_access_path": volume_access_path
        }))
