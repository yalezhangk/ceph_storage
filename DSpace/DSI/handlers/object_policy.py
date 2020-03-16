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


create_object_policy_schema = {
    "type": "object",
    "properties": {
        "object_policy": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "minLength": 5,
                    "maxLength": 32
                },
                "description": {
                    "type": "string",
                    "minLength": 0,
                    "maxLength": 255
                },
                "index_pool_id": {"type": "integer"},
                "data_pool_id": {"type": "integer"},
                "compression": {
                    "type": "string",
                    "minLength": 0,
                    "maxLength": 36
                },
            },
            "required": ["name", "index_pool_id", "data_pool_id"],
        },
    },
    "required": ["object_policy"],
}

update_object_policy_schema = {
    "type": "object",
    "properties": {
        "object_policy": {
            "type": "object",
            "properties": {
                "description": {
                    "type": "string",
                    "minLength": 0,
                    "maxLength": 255
                },
            },
            "required": ["description"],
        },
    },
    "required": ["object_policy"],
}


@URLRegistry.register(r"/object_policies/")
class ObjectPolicyListHandler(ClusterAPIHandler):

    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - object_policy
        summary: object_policy List
        description: Return a list of object_policies
        operationId: object_policy.api.listObjectPolicy
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
        expected_attrs = ['index_pool', 'data_pool', 'buckets']
        joined_load = self.get_query_argument('joined_load', default=None)
        if joined_load == '0':
            expected_attrs = []
        fuzzy_filters = ['name']
        filters = self.get_support_filters(fuzzy_filters=fuzzy_filters)

        object_policies = yield client.object_policy_get_all(
            ctxt, expected_attrs=expected_attrs, filters=filters, **page_args)
        count = yield client.object_policy_get_count(ctxt, filters=filters)
        self.write(objects.json_encode({
            "object_policies": object_policies,
            "total": count
        }))

    @gen.coroutine
    def post(self):
        """Create object_policy

        ---
        tags:
        - object_policy
        summary: Create object_policy
        description: Create object_policy.
        operationId: object_policies.api.create_object_policy
        produces:
        - application/json
        parameters:
        - in: body
          name: object_policy
          description: Created volume object
          required: true
          schema:
            type: object
            properties:
              name:
                type: string
                description: name
              description:
                type: string
                description: description
              index_pool_id:
                type: integer
                format: int32
                description: index_pool object.id
              data_pool_id:
                type: integer
                format: int32
                description: data_pool object.id
              compression:
                type: string
                description: compression algorithm or null
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=create_object_policy_schema,
                 format_checker=draft7_format_checker)
        data = data.get("object_policy")
        client = self.get_admin_client(ctxt)
        policy = yield client.object_policy_create(ctxt, data)
        self.write(objects.json_encode({
            "object_policy": policy
        }))


@URLRegistry.register(r"/object_policies/compressions/")
class PolicyCompressionListHandler(ClusterAPIHandler):

    @gen.coroutine
    def get(self):
        """CompressionAlgorithm List

        ---
        tags:
        - CompressionAlgorithm
        summary: CompressionAlgorithm
        description: Return CompressionAlgorithm List
        operationId: object_policies.api.compressions
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        compressions = yield client.policy_get_all_compressions(ctxt)
        self.write(objects.json_encode({
            "compressions": compressions
        }))


@URLRegistry.register(r"/object_policies/([0-9]*)/")
class ObjectPolicyHandler(ClusterAPIHandler):

    @gen.coroutine
    def get(self, object_policy_id):
        """ObjectPolicy Detail

        ---
        tags:
        - object_policy
        summary: Detail of the object_policy
        description: Return detail infomation of object_policy by id
        operationId: object_policies.api.object_policy_detail
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
          description: ObjectPolicy ID
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
        expected_attrs = ['index_pool', 'data_pool', 'buckets']
        object_policy = yield client.object_policy_get(
            ctxt, object_policy_id, expected_attrs=expected_attrs)
        self.write(objects.json_encode({"object_policy": object_policy}))

    @gen.coroutine
    def put(self, object_policy_id):
        """Update ObjectPolicy

        ---
        tags:
        - object_policy
        summary: update object_policy
        description: update object_policy description
        produces:
        - application/json
        parameters:
        - in: body
          name: object_policy
          description: update object_policy description
          required: true
          schema:
            type: object
            properties:
              description:
                type: string
                description: new description
        responses:
        "200":
          description: successful operation
        """
        # 更改描述
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=update_object_policy_schema,
                 format_checker=draft7_format_checker)
        object_policy_data = data.get('object_policy')
        client = self.get_admin_client(ctxt)
        object_policy = yield client.object_policy_update(
            ctxt, object_policy_id, object_policy_data)
        self.write(objects.json_encode({
            "object_policy": object_policy
        }))

    @gen.coroutine
    def delete(self, object_policy_id):
        """Delete object_policy

        ---
        tags:
        - object_policy
        summary: delete object_policy
        produces:
        - application/json
        parameters:
        - in: URL
          name: id
          description: object_policy id
          required: true
          schema:
            type: int
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        object_policy = yield client.object_policy_delete(
            ctxt, object_policy_id)
        self.write(objects.json_encode({
            "object_policy": object_policy
        }))


@URLRegistry.register(r"/object_policies/([0-9]*)/action/")
class ObjectPolicyActionHandler(ClusterAPIHandler):

    @gen.coroutine
    def _set_default(self, client, ctxt, object_policy_id, policy_data):
        default = yield client.object_policy_set_default(
            ctxt, object_policy_id)
        return default

    @gen.coroutine
    def _set_compression(self, client, ctxt, object_policy_id, policy_data):
        compression = yield client.object_policy_set_compression(
            ctxt, object_policy_id, policy_data)
        return compression

    @gen.coroutine
    def put(self, object_policy_id):
        """object_policy action:default、compression

        ---
        tags:
        - object_policy
        summary: default(设置缺省策略)、compression(设置数据压缩)
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
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        policy_data = data.get('object_policy')
        action = data.get('action')
        client = self.get_admin_client(ctxt)
        map_action = {
            'default': self._set_default,
            'compression': self._set_compression,
        }
        fun_action = map_action.get(action)
        if fun_action is None:
            raise exception.ObjectPolicyActionNotFound(action=action)
        result = yield fun_action(client, ctxt, object_policy_id, policy_data)
        self.write(objects.json_encode({
            "object_policy": result
        }))
