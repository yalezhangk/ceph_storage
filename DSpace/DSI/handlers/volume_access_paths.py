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

logger = logging.getLogger(__name__)


volume_access_path_schema = {
    "type": "object",
    "properties": {
        "access_path": {
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
                }
            },
            "required": ["name"]
        }
    },
    "additionalProperties": False,
    "required": ["access_path"]
}

update_volume_access_path_gw_schema = {
    "type": "object",
    "properties": {
        "access_path": {
            "type": "object",
            "properties": {
                "node_id": {
                    "type": "integer",
                    "minimum": 1
                },
            },
            "required": ["node_id"]
        }
    },
    "additionalProperties": False,
    "required": ["access_path"]
}

update_volume_access_path_mapping_schema = {
    "type": "object",
    "properties": {
        "access_path": {
            "type": "object",
            "properties": {
                "mapping_list": {
                    "type": "array",
                    "items": {"type": "integer", "minimum": 1},
                    "minItems": 1,
                    "uniqueItems": True
                },
            },
            "required": ["mapping_list"]
        }
    },
    "additionalProperties": False,
    "required": ["access_path"]
}

volume_access_path_set_chap_schema = {
    "type": "object",
    "properties": {
        "access_path": {
            "type": "object",
            "properties": {
                "chap_enable": {"type": "boolean"},
            },
            "if": {
                "properties": {"chap_enable": {"const": True}}
            },
            "then": {
                "properties": {
                    "username": {
                        "type": "string",
                        "minLength": 5,
                        "maxLength": 32
                    },
                    "password": {
                        "type": "string",
                        "minLength": 5,
                        "maxLength": 32
                    },
                },
                "required": ["username", "password"]
            },
            "required": ["chap_enable"]
        }
    },
    "additionalProperties": False,
    "required": ["access_path"]
}

vap_change_client_group_schema = {
    "type": "object",
    "properties": {
        "access_path": {
            "type": "object",
            "properties": {
                "client_group_id": {"type": "integer", "minimum": 1},
                "new_client_group_id": {"type": "integer", "minimum": 1},
            },
            "required": ["client_group_id", "new_client_group_id"]
        }
    },
    "additionalProperties": False,
    "required": ["access_path"]
}

update_access_path_volumes_schema = {
    "type": "object",
    "properties": {
        "access_path": {
            "type": "object",
            "properties": {
                "client_group_id": {"type": "integer", "minimum": 1},
                "volume_ids": {
                    "type": "array",
                    "items": {"type": "integer", "minimum": 1},
                    "minItems": 1,
                    "uniqueItems": True
                },
            },
            "required": ["client_group_id", "volume_ids"]
        }
    },
    "additionalProperties": False,
    "required": ["access_path"]
}


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
            logger.error("create access path error, %s already exists", name)
            raise exception.Duplicate(
                _("volume_access_path: {} is already exists!").format(name))

    @gen.coroutine
    def check_volume_access_path_chap(self, ctxt, data):
        username = data.get("username")
        password = data.get("password")
        if not username or not password:
            raise exception.InvalidInput(_("no username or password input"))
        if len(username) < 5 or len(password) < 5:
            raise exception.InvalidInput(_("username or password too short"))
        elif len(username) > 32 or len(password) > 32:
            raise exception.InvalidInput(_("username or password too long"))


@URLRegistry.register(r"/volume_access_paths/")
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

        logger.info("trying to create a new access path")
        data = json_decode(self.request.body)
        validate(data, schema=volume_access_path_schema,
                 format_checker=draft7_format_checker)
        data = data.get('access_path')
        if not data:
            logger.error("create access path error, no data access_path input")
            raise exception.InvalidInput(_("no access_path input"))
        name = data.get("name")
        type = data.get("type")
        if not name or not type:
            logger.error("create access path error, no name or type input")
            raise exception.InvalidInput(_("name or type not found"))
        yield self.check_volume_access_path_by_name(ctxt, client, name)
        volume_access_path = yield client.volume_access_path_create(
            ctxt, data)
        logger.info("create access path success, %s", volume_access_path)
        self.write(objects.json_encode({
            "volume_access_path": volume_access_path
        }))


@URLRegistry.register(r"/volume_access_paths/([0-9]*)/")
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

        logger.info("trying to edit access path")
        yield self.check_volume_access_path_by_id(ctxt, client, id)
        data = json_decode(self.request.body)
        validate(data, schema=volume_access_path_schema,
                 format_checker=draft7_format_checker)
        data = data.get('access_path')
        if not data.get('name'):
            logger.error("edit volume access path error, no name input")
            raise exception.InvalidInput(_("name or type not found"))
        yield self.check_volume_access_path_by_name(
            ctxt, client, data.get('name'))

        volume_access_path = yield client.volume_access_path_update(
            ctxt, id, data)
        logger.info("edit access path success: %s", volume_access_path)
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
        logger.info("trying to delete access path")
        yield self.check_volume_access_path_by_id(ctxt, client, id)
        volume_access_path = yield client.volume_access_path_delete(ctxt, id)
        logger.info("delete access path success")
        self.write(objects.json_encode({
            "volume_access_path": volume_access_path
        }))


@URLRegistry.register(r"/volume_access_paths/([0-9]*)/mount_gw/")
class VolumeAccessPathMountGWHandler(ClusterAPIHandler, CheckVolumeAccessPath):
    @gen.coroutine
    def post(self, id):
        """挂载网关服务器
        {"access_path":{"node_id":1}}

        ---
        tags:
        - volume_access_path
        summary: Mount block gateway
        description: Mount block gateway
        operationId: volume_access_paths.api.mountBGW
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
          description: Mount block gateway
          required: true
          schema:
            type: object
            properties:
              access_path:
                type: object
                properties:
                  node_id:
                    type: integer
                    format: int32
                    description: node's id
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)

        yield self.check_volume_access_path_by_id(ctxt, client, id)
        data = json_decode(self.request.body)
        validate(data, schema=update_volume_access_path_gw_schema,
                 format_checker=draft7_format_checker)
        data = data.get('access_path')
        logger.info("trying to mount block gateway")
        if not data.get('node_id'):
            logger.error("mount bgw error, no node_id input")
            raise exception.InvalidInput(_("no node_id input"))
        logger.debug("mount_gw, data {}".format(data))
        ap_gateway = yield client.volume_access_path_mount_gw(
            ctxt, id, data)
        logger.info("mount block gateway success")
        self.write(objects.json_encode({
            "ap_gateway": ap_gateway
        }))


@URLRegistry.register(r"/volume_access_paths/([0-9]*)/unmount_gw/")
class VolumeAccessPathUnmountGWHandler(ClusterAPIHandler,
                                       CheckVolumeAccessPath):
    @gen.coroutine
    def post(self, id):
        """卸载网关服务器
        {"access_path":{"node_id":1}}

        ---
        tags:
        - volume_access_path
        summary: Unmount block gateway
        description: Unmount block gateway
        operationId: volume_access_paths.api.UnmountBGW
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
          description: Unmount block gateway
          required: true
          schema:
            type: object
            properties:
              access_path:
                type: object
                properties:
                  node_id:
                    type: integer
                    format: int32
                    description: node's id
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)

        yield self.check_volume_access_path_by_id(ctxt, client, id)

        logger.info("trying to unmount block gateway")
        data = json_decode(self.request.body)
        validate(data, schema=update_volume_access_path_gw_schema,
                 format_checker=draft7_format_checker)
        data = data.get('access_path')
        if not data.get('node_id'):
            raise exception.InvalidInput(_("no node_id input"))
        logger.debug("unmount_gw, data %s", data)
        access_path = yield client.volume_access_path_unmount_gw(
            ctxt, id, data)
        logger.info("unmount block gateway success: %s", access_path)
        self.write(objects.json_encode({
            "access_path": access_path
        }))


@URLRegistry.register(r"/volume_access_paths/([0-9]*)/create_mapping/")
class VolumeAccessPathCreateMappingHandler(ClusterAPIHandler,
                                           CheckVolumeAccessPath):
    @gen.coroutine
    def post(self, id):
        """添加映射
        {"access_path":{"mapping_list":[
            {"client_group_id":1,"volume_ids":[1,2]},
            {"client_group_id":2,"volume_ids":[1,2]},
            {"client_group_id":3,"volume_ids":[1,3]}
            ]}
        }

        ---
        tags:
        - volume_access_path
        summary: Create volume mappings
        description: Create volume mappings
        operationId: volume_access_paths.api.createMapping
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
          description: create mapping access_path
          required: true
          schema:
            type: object
            description: volume_access_path object
            properties:
              mapping_list:
                type: array
                items:
                  type: object
                  properties:
                    client_group_id:
                      type: integer
                      format: int32
                    volume_ids:
                      type: array
                      items:
                        type: integer
                        format: int32
                        description: volume ids

        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)

        yield self.check_volume_access_path_by_id(ctxt, client, id)
        logger.info("trying to create volume mapping")
        data = json_decode(self.request.body)
        validate(data, schema=update_volume_access_path_mapping_schema,
                 format_checker=draft7_format_checker)
        data = data.get('access_path')
        mapping_list = data.get("mapping_list")
        logger.debug("create mapping, data {}".format(mapping_list))
        access_path = yield client.volume_access_path_create_mapping(
            ctxt, id, mapping_list)
        logger.info("create volume mapping success")
        self.write(objects.json_encode({
            "access_path": access_path
        }))


@URLRegistry.register(r"/volume_access_paths/([0-9]*)/remove_mapping/")
class VolumeAccessPathRemoveMappingHandler(ClusterAPIHandler,
                                           CheckVolumeAccessPath):
    @gen.coroutine
    def post(self, id):
        """移除映射
        {"access_path":{"mapping_list":[
            {"client_group_id":1,"volume_ids":[]},
            {"client_group_id":2,"volume_ids":[]},
            {"client_group_id":3,"volume_ids":[]}
            ]}
        }

        ---
        tags:
        - volume_access_path
        summary: Remove volume mappings
        description: Remove volume mappings
        operationId: volume_access_paths.api.removeMapping
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
          description: access_path remove mapping
          required: true
          schema:
            type: object
            description: volume_access_path object
            properties:
              mapping_list:
                type: array
                items:
                  type: object
                  properties:
                    client_group_id:
                      type: integer
                      format: int32
                    volume_ids:
                      type: array
                      items:
                        type: integer
                        format: int32
                        description: volume ids

        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)

        logger.info("trying to remove volume mapping")
        yield self.check_volume_access_path_by_id(ctxt, client, id)
        data = json_decode(self.request.body)
        validate(data, schema=update_volume_access_path_mapping_schema,
                 format_checker=draft7_format_checker)
        data = data.get('access_path')
        mapping_list = data.get("mapping_list")
        logger.debug("remove mapping, data {}".format(mapping_list))
        access_path = yield client.volume_access_path_remove_mapping(
            ctxt, id, mapping_list)
        logger.info("remove volume mapping success")
        self.write(objects.json_encode({
            "access_path": access_path
        }))


@URLRegistry.register(r"/volume_access_paths/([0-9]*)/set_chap/")
class VolumeAccessPathChapHandler(ClusterAPIHandler,
                                  CheckVolumeAccessPath):
    @gen.coroutine
    def post(self, id):
        """开启/关闭CHAP
        {"access_path":{"chap_enable":false}}

        {"access_path":
            {"chap_enable":true,"username":"username","password":"password"}
        }

        ---
        tags:
        - volume_access_path
        summary: Enable/Disable Chap
        description: Enable/Disable Chap
        operationId: volume_access_paths.api.setChap
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
          name: access_path(enable chap)
          description: access_path set chap
          required: true
          schema:
            type: object
            description: volume_access_path object
            properties:
              chap_enable:
                type: boolean
                description: enable chap
              username:
                type: string
                description: chap usernamep
              password:
                type: string
                description: chap password
        - in: body
          name: access_path(disable chap)
          description: access_path set chap
          required: true
          schema:
            type: object
            description: volume_access_path object
            properties:
              chap_enable:
                type: boolean
                description: disable chap, it must be fals

        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)

        logger.info("trying to set chap")
        yield self.check_volume_access_path_by_id(ctxt, client, id)
        data = json_decode(self.request.body)
        validate(data, schema=volume_access_path_set_chap_schema,
                 format_checker=draft7_format_checker)
        data = data.get('access_path')
        if "chap_enable" not in data:
            logger.error("no chap_enable in request body")
            raise exception.InvalidInput(_("no chap_enable input"))
        if data.get("chap_enable"):
            yield self.check_volume_access_path_chap(ctxt, data)
        logger.debug("set target chap, data %s", data)
        access_path = yield client.volume_access_path_set_chap(ctxt, id, data)
        logger.info("set chap success")
        self.write(objects.json_encode({
            "access_path": access_path
        }))


@URLRegistry.register(r"/volume_access_paths/([0-9]*)/change_client_group/")
class VolumeAccessPathChangeClientGroupHandler(ClusterAPIHandler,
                                               CheckVolumeAccessPath):
    @gen.coroutine
    def post(self, id):
        """修改客户端组，新的客户端组还使用旧的客户端组所用的块存储卷
        {"access_path":{"client_group_id":1,"new_client_group_id":2}}

        ---
        tags:
        - volume_access_path
        summary: Edit access_path's client group
        description: Edit access_path's client group
        operationId: volume_access_paths.api.editClientGroup
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
          name: access_path(edit client group)
          description: edit client group
          required: true
          schema:
            type: object
            description: volume_access_path object
            properties:
              client_group_id:
                type: integer
                format: int32
                description: old client group id
              new_client_group_id:
                type: integer
                format: int32
                description: new client group id
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        logger.info("trying to change client group")
        yield self.check_volume_access_path_by_id(ctxt, client, id)
        data = json_decode(self.request.body)
        validate(data, schema=vap_change_client_group_schema,
                 format_checker=draft7_format_checker)
        data = data.get('access_path')
        if "client_group_id" not in data:
            raise exception.InvalidInput(_("no client_group_id input"))
        logger.debug("change client group, data %s", data)
        access_path = yield client.volume_access_path_change_client_group(
            ctxt, id, data)
        logger.info("change client group success")
        self.write(objects.json_encode({
            "access_path": access_path
        }))


@URLRegistry.register(r"/volume_access_paths/([0-9]*)/add_volume/")
class VolumeAccessPathAddVolumeHandler(ClusterAPIHandler,
                                       CheckVolumeAccessPath):
    @gen.coroutine
    def post(self, id):
        """往某个映射中添加块存储卷
        {"access_path":{"client_group_id":1, volume_ids":[1,2]}}

        ---
        tags:
        - volume_access_path
        summary: Add volumes to mapping
        description: Add volumes to mapping
        operationId: volume_access_paths.api.addVolumes
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
          name: access_path(add volumes to mapping)
          description: add volumes to mapping
          required: true
          schema:
            type: object
            description: volume_access_path object
            properties:
              client_group_id:
                type: integer
                format: int32
                description: old client group id
              volume_ids:
                type: array
                items:
                  type: integer
                  format: int32
                  description: volume ids
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)

        logger.info("trying to add volume to client group")
        yield self.check_volume_access_path_by_id(ctxt, client, id)
        data = json_decode(self.request.body)
        validate(data, schema=update_access_path_volumes_schema,
                 format_checker=draft7_format_checker)
        data = data.get('access_path')
        if "volume_ids" not in data:
            logger.error("no volume_ids in request data")
            raise exception.InvalidInput(_("no volume_ids input"))
        if "client_group_id" not in data:
            logger.error("no client_group_id in request data")
            raise exception.InvalidInput(_("no client_group_id input"))
        logger.debug("access path add volume, data %s", data)
        access_path = yield client.volume_access_path_add_volume(
            ctxt, id, data)
        logger.info("add volume to client group success")
        self.write(objects.json_encode({
            "access_path": access_path
        }))


@URLRegistry.register(r"/volume_access_paths/([0-9]*)/remove_volume/")
class VolumeAccessPathRemoveVolumeHandler(ClusterAPIHandler,
                                          CheckVolumeAccessPath):
    @gen.coroutine
    def post(self, id):
        """往某个映射中移除块存储卷
        {"access_path":{"client_group_id":1, volume_ids":[1,2]}}

        ---
        tags:
        - volume_access_path
        summary: Remove volumes from mapping
        description: Remove volumes from mapping
        operationId: volume_access_paths.api.removeVolumes
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
          name: access_path(add volumes to mapping)
          description: add volumes to mapping
          required: true
          schema:
            type: object
            description: volume_access_path object
            properties:
              client_group_id:
                type: integer
                format: int32
                description: old client group id
              volume_ids:
                type: array
                items:
                  type: integer
                  format: int32
                  description: volume ids
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)

        yield self.check_volume_access_path_by_id(ctxt, client, id)
        data = json_decode(self.request.body)
        validate(data, schema=update_access_path_volumes_schema,
                 format_checker=draft7_format_checker)
        data = data.get('access_path')
        if "volume_ids" not in data:
            logger.error("no volume_ids in request data")
            raise exception.InvalidInput(_("no volume_ids input"))
        if "client_group_id" not in data:
            logger.error("no client_group_id in request data")
            raise exception.InvalidInput(_("no client_group_id input"))
        logger.debug("access path remove volume, data %s", data)
        access_path = yield client.volume_access_path_remove_volume(
            ctxt, id, data)
        self.write(objects.json_encode({
            "access_path": access_path
        }))
