import logging

from jsonschema import draft7_format_checker
from jsonschema import validate
from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler
from DSpace.i18n import _
from DSpace.utils.normalize_wwn import normalize_wwn

logger = logging.getLogger(__name__)


create_volume_client_group_schema = {
    "type": "object",
    "properties": {
        "volume_client_group": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "minLength": 5,
                    "maxLength": 32
                },
                "type": {
                    "type": "string",
                    "enum": ["iscsi"]
                },
                "clients": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": ["iqn"]
                            },
                            "iqn": {"type": "string"}
                        },
                        "required": ["type", "iqn"]
                    },
                    "minItems": 1,
                    "uniqueItems": True
                },
            },
            "required": ["name", "type", "clients"]
        }
    },
    "additionalProperties": False,
    "required": ["volume_client_group"]
}

update_volume_client_group_schema = {
    "type": "object",
    "properties": {
        "volume_client_group": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "minLength": 5,
                    "maxLength": 32
                },
                "type": {
                    "type": "string",
                    "enum": ["iscsi"]
                },
                "clients": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": ["iqn"]
                            },
                            "iqn": {"type": "string"}
                        },
                        "required": ["type", "iqn"]
                    },
                    "minItems": 1,
                    "uniqueItems": True
                },
            },
            "dependencies": {
                "type": ["clients"],
                "clients": ["type"]
            }
        }
    },
    "additionalProperties": False,
    "required": ["volume_client_group"]
}

vcp_set_mutual_chap_schema = {
    "type": "object",
    "properties": {
        "volume_client_group": {
            "type": "object",
            "properties": {
                "mutual_chap_enable": {"type": "boolean"},
            },
            "if": {
                "properties": {"mutual_chap_enable": {"const": True}}
            },
            "then": {
                "properties": {
                    "mutual_username": {
                        "type": "string",
                        "minLength": 5,
                        "maxLength": 32
                    },
                    "mutual_password": {
                        "type": "string",
                        "minLength": 5,
                        "maxLength": 32
                    },
                },
                "required": ["mutual_username", "mutual_password"]
            },
            "required": ["mutual_chap_enable"]
        }
    },
    "additionalProperties": False,
    "required": ["volume_client_group"]
}


class CheckVolumeClientGroup():
    @gen.coroutine
    def check_volume_client_group(self, ctxt, client, volume_client_group):
        """verify that the volume cilent group's name exists."""
        name = volume_client_group.get('name')
        filters = {"name": name}
        vcg = yield client.volume_client_group_get_all(ctxt, filters=filters)
        if vcg:
            logger.error("volume client group {} exists".format(vcg))
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
                logger.error("iqn %s is not allowed", iqn)
                raise exception.Invalid(
                    _("iqn: {} is not allowed!").format(iqn))

            vc = yield client.volume_client_get_all(ctxt, filters=filters)
            if vc:
                logger.error("volume client %s already in another "
                             "cilent group", iqn)
                raise exception.VolumeClientExists(iqn=iqn)

    @gen.coroutine
    def check_volume_client_group_mutual_chap(self, ctxt, data):
        mutual_username = data.get("mutual_username")
        mutual_password = data.get("mutual_password")
        if not mutual_username or not mutual_password:
            raise exception.InvalidInput(_("no mutual_username or "
                                           " mutual_password input"))
        if len(mutual_username) < 5 or len(mutual_password) < 5:
            raise exception.InvalidInput(_("mutual_username or mutual_password"
                                           "too short"))
        elif len(mutual_username) > 32 or len(mutual_password) > 32:
            raise exception.InvalidInput(_("mutual_username or mutual_password"
                                           "too long"))


@URLRegistry.register(r"/volume_client_groups/")
class VolumeClientGroupListHandler(ClusterAPIHandler, CheckVolumeClientGroup):
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
        expected_attrs = ['volume_access_paths', 'volumes', 'volume_clients']

        exact_filters = ['type']
        fuzzy_filters = ['name']
        filters = self.get_support_filters(exact_filters, fuzzy_filters)

        volume_client_groups = yield client.volume_client_group_get_all(
            ctxt, expected_attrs=expected_attrs, filters=filters, **page_args)
        volume_client_group_count = \
            yield client.volume_client_group_get_count(ctxt, filters=filters)
        self.write(objects.json_encode({
            "volume_client_group": volume_client_groups,
            "total": volume_client_group_count
        }))

    @gen.coroutine
    def post(self):
        """
        {
            "volume_client_group": {
                "name":"client-group-001",
                "type":"iscsi",
                "chap_enable": false,
                "chap_name": "chap_name",
                "chap_password": "chap_password",
                "clients": [
                    {
                        "type": "iqn",
                        "iqn": "iqn.1994-05.com.redhat:645ae6375e70"
                    },
                    {
                        "type": "iqn",
                        "iqn": "iqn.1994-05.com.redhat:645ae6375e71"
                    }
                ]}
        }

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
        data = json_decode(self.request.body)
        validate(data, schema=create_volume_client_group_schema,
                 format_checker=draft7_format_checker)
        volume_client_group = data.get('volume_client_group')
        volume_clients = volume_client_group.pop("clients")

        # 验证
        # check方法返回的是Futrue，所以这里还需要一个yield来推动
        logger.info("trying to create volume client group")
        yield self.check_volume_client_group(ctxt, client, volume_client_group)
        for volume_client in volume_clients:
            yield self.check_volume_client(ctxt, client, volume_client)
        # 创建
        logger.debug("create volume client group, data: %s",
                     volume_client_group)
        volume_client_group = yield client.volume_client_group_create(
            ctxt, volume_client_group)
        for volume_client in volume_clients:
            volume_client["volume_client_group_id"] = volume_client_group.id
            yield client.volume_client_create(ctxt, volume_client)
        logger.info("create volume client group success")
        self.write(objects.json_encode({
            "volume_client_group": volume_client_group
        }))


@URLRegistry.register(r"/volume_client_groups/([0-9]*)/")
class VolumeClientGroupHandler(ClusterAPIHandler, CheckVolumeClientGroup):
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
        expected_attrs = ['volume_access_paths', 'volumes', 'volume_clients']
        volume_client_group = yield client.volume_client_group_get(
            ctxt, group_id, expected_attrs=expected_attrs)
        self.write(objects.json_encode(
            {"volume_client_group": volume_client_group}))

    @gen.coroutine
    def post(self, group_id):
        """编辑客户端组：修改客户端组名称
        {"volume_client_group": {"name":"new_client_group"}}

        添加或移除客户端
        {
            "volume_client_group": {
                "type":"iscsi",
                "clients": [
                    {
                        "type": "iqn",
                        "iqn": "iqn.1994-05.com.redhat:645ae6375e70"
                    }
                ]}
        }

        ---
        tags:
        - volume_client_group
        summary: Edit client group name or update clients
        description: Edit client group name or update clients
        operationId: volume_client_groups.api.editClientGroup
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
        - in: body
          name: volume_client_group(name)
          description: Edit volume client group name
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
                    description: volume client group name
        - in: body
          name: volume_client_group(update volume clients)
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
                    description: volume client group name
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
        data = json_decode(self.request.body)
        validate(data, schema=update_volume_client_group_schema,
                 format_checker=draft7_format_checker)
        client_group = data.get('volume_client_group')
        # Only change client group name
        if "name" in client_group:
            volume_client_group = yield client.volume_client_group_update_name(
                ctxt, group_id, client_group)

        if "clients" in client_group:
            volume_clients = client_group.pop("clients")
            volume_client_group = yield client.volume_client_group_update(
                ctxt, group_id, volume_clients)
        self.write(objects.json_encode({
            "volume_client_group": volume_client_group
        }))

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


@URLRegistry.register(r"/volume_client_groups/([0-9]*)/clients/")
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


@URLRegistry.register(r"/volume_client_groups/([0-9]*)/set_mutual_chap/")
class VolumeClientGroupSetMutualChapHandler(ClusterAPIHandler,
                                            CheckVolumeClientGroup):
    @gen.coroutine
    def post(self, group_id):
        """开启/关闭双向CHAP
        {"volume_client_group":{"mutual_chap_enable":false}}

        {
            "volume_client_group":
            {
                "mutual_chap_enable":true,
                "mutual_username":"username",
                "mutual_password":"password"
            }
        }

        ---
        tags:
        - volume_client_group
        summary: Enable/Disable mutual CHAP
        description: Enable/Disable mutual CHAP
        operationId: volume_client_groups.api.setMutualChap
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
        - in: body
          name: volume_client_group(enable mutual chap)
          description: Created volume_client_group object
          required: true
          schema:
            type: object
            properties:
              volume_client_group:
                type: object
                description: volume_client_group object
                properties:
                  mutual_chap_enable:
                    type: boolean
                    description: enable mutual chap
                  mutual_username:
                    type: string
                    description: mutual username
                  mutual_password:
                    type: string
                    description: mutual password
        - in: body
          name: volume_client_group(disable mutual chap)
          description: Created volume_client_group object
          required: true
          schema:
            type: object
            properties:
              volume_client_group:
                type: object
                description: volume_client_group object
                properties:
                  mutual_chap_enable:
                    type: boolean
                    description: disable mutual chap, it must be false
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        data = json_decode(self.request.body)
        validate(data, schema=vcp_set_mutual_chap_schema,
                 format_checker=draft7_format_checker)
        data = data.get('volume_client_group')
        logger.debug("set client group mutual chap, data %s", data)
        if "mutual_chap_enable" not in data:
            logger.error("no mutual_chap_enable in request body")
            raise exception.InvalidInput(_("no mutual_chap_enable input"))
        if data.get("mutual_chap_enable"):
            yield self.check_volume_client_group_mutual_chap(ctxt, data)
        client_group = yield client.set_mutual_chap(
            ctxt, group_id, data)
        self.write(objects.json_encode({
            "volume_client_group": client_group
        }))
