import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.DSI.handlers.base import ClusterAPIHandler
from DSpace.i18n import _
from DSpace.utils.normalize_wwn import normalize_wwn

logger = logging.getLogger(__name__)


class VolumeClientGroupListHandler(ClusterAPIHandler):
    @gen.coroutine
    def check_volume_client_group(self, ctxt, client, volume_client_group):
        """verify that the volume cilent group's name exists."""
        name = volume_client_group.get('name')
        filters = {"name": name}
        vcg = yield client.volume_client_group_get_all(ctxt, filters=filters)
        if vcg:
            logger.error("Im raise a exception")
            raise exception.VolumeClientGroupExists(
                volume_client_group_name=name)

    @gen.coroutine
    def check_volume_client(self, ctxt, client, volume_client):
        """verify that the volume cilent's name exists."""
        iqn = volume_client.get('iqn')
        filters = {"iqn": iqn}
        if iqn:
            normalize_iqn, wwn_type = normalize_wwn(iqn)
            if not normalize_iqn:
                raise exception.Invalid(
                    _("iqn: {} is not allowed!").format(iqn))

            vc = yield client.volume_client_get_all(ctxt, filters=filters)
            if vc:
                raise exception.VolumeClientExists(iqn=iqn)

    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - volume_client_group
        summary: Volume Client Group List
        description: Return a list of Volume Client Group
        operationId: volume_client_groups.api.listVolumeClientGroup
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
        expected_attrs = ['volume_access_path', 'volumes', 'volume_clients']
        volume_client_groups = yield client.volume_client_group_get_all(
            ctxt, expected_attrs=expected_attrs, **page_args)
        volume_client_group_count = \
            yield client.volume_client_group_get_count(ctxt)
        self.write(objects.json_encode({
            "volume_client_group": volume_client_groups,
            "total": volume_client_group_count
        }))

    @gen.coroutine
    def post(self):
        """
        ---
        tags:
        - volume_client_group
        summary: Create volume_client_group
        description: Create volume_client_group.
        operationId: volume_client_groups.api.createVolumeClientGroup
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
          name: volume_client_group
          description: Created volume_client_group object
          required: true
          schema:
            type: object
            properties:
              volume_client_group:
                type: object
                description: volume_client_group object
                properties:
                  name:
                    type: string
                    description: volume_client_group's name
                  type:
                    type: string
                    description: volume_client_group's type, it can be iSCSI
                  chap_enable:
                    type: boolean
                  chap_username:
                    type: string
                  chap_password:
                    type: string
                  clients:
                    type: array
                    items:
                      type: object
                      properties:
                        client_type:
                          type: string
                          description: volume_client's type, it can be iqn
                        iqn:
                          type: string
                          description: volume_client's iqn, it must be in
                                        strict iqn format
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        volume_client_group = json_decode(
            self.request.body).get('volume_client_group')
        volume_clients = volume_client_group.pop("clients")

        # 验证
        # check方法返回的是Futrue，所以这里还需要一个yield来推动
        yield self.check_volume_client_group(ctxt, client, volume_client_group)
        for volume_client in volume_clients:
            yield self.check_volume_client(ctxt, client, volume_client)
        # 创建
        volume_client_group = \
            yield client.volume_client_group_create(ctxt, volume_client_group)
        for volume_client in volume_clients:
            volume_client["volume_client_group_id"] = volume_client_group.id
            yield client.volume_client_create(ctxt, volume_client)
        self.write(objects.json_encode({
            "volume_client_group": volume_client_group
        }))


class VolumeClientGroupHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, group_id):
        """
        ---
        tags:
        - volume_client_group
        summary: Detail of the volume_client_group
        description: Return detail infomation of volume_client_group by id
        operationId: volume_client_group.api.VolumeClientGroupDetail
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
          description: VolumeClientGroup's id
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
        expected_attrs = ['volume_access_path', 'volumes', 'volume_clients']
        volume_client_group = yield client.volume_client_group_get(
            ctxt, group_id, expected_attrs=expected_attrs)
        self.write(objects.json_encode(
            {"volume_client_group": volume_client_group}))

    @gen.coroutine
    def delete(self, group_id):
        """删除客户端组
        ---
        tags:
        - volume_client_group
        summary: Delete the volume_client_group by id
        description: delete volume_client_group by id
        operationId: volume_client_group.api.deleteVolumeClientGroup
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
          description: VolumeClientGroup's id
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
        volume_client_group = yield client.volume_client_group_delete(
            ctxt, group_id)
        self.write(objects.json_encode(
            {"volume_client_group": volume_client_group}))


class VolumeClientByGroup(ClusterAPIHandler):
    @gen.coroutine
    def get(self, group_id):
        """获取客户端组下的所有客户端组
        ---
        tags:
        - volume_client_group
        summary: Return clients of volume client group
        description: Return the clients of volume_client_group by
                     volume_client_group id
        operationId: volume_client_group.api.getCLientOfVolumeClientGroup
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
          description: VolumeClientGroup's id
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
        filters = {"volume_client_group_id": group_id}
        volume_clients = yield client.volume_client_get_all(
            ctxt, filters=filters)
        self.write(objects.json_encode({"volume_clients": volume_clients}))
