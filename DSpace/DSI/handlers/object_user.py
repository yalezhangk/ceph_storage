import logging

from jsonschema import draft7_format_checker
from jsonschema import validate
from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


create_object_user_schema = {
    "type": "object",
    "properties": {
        "object_user": {
            "type": "object",
            "properties": {
                "uid": {
                    "type": "string",
                    "minLength": 5,
                    "maxLength": 32
                },
                "email": {
                    "type": "string",
                    "format": "email"
                },
                "max_buckets": {"type": "number",
                                "minimum": 0,
                                "maximum": 10000},
                "user_quota_max_size": {"type": "number"},
                "user_quota_max_objects": {"type": "number"},
                "bucket_quota_max_size": {"type": "number"},
                "bucket_quota_max_objects": {"type": "number"},
                "description": {
                    "type": "string",
                    "minLength": 0,
                    "maxLength": 255
                },
            },
            "required": ["uid", "max_buckets", "user_quota_max_size",
                         "user_quota_max_objects",
                         "bucket_quota_max_size",
                         "bucket_quota_max_objects"],
        },
    },
    "required": ["object_user"],
}

update_object_user_schema = {
    "type": "object",
    "properties": {
        "object_user": {
            "type": "object",
            "properties": {
                "suspended": {
                    "type": "boolean"},
            },
            "required": ["suspended"],
        },
    },
    "required": ["object_user"],
}


@URLRegistry.register(r"/object_user/")
class ObjectUserListHandler(ClusterAPIHandler):

    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - object_user
        summary: object_user List
        description: Return a list of object_users
        operationId: object_user.api.listObjectObject
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
        expected_attrs = ['access_keys']
        fuzzy_filters = ['name']
        filters = self.get_support_filters(fuzzy_filters=fuzzy_filters)

        object_users = yield client.object_user_get_all(
            ctxt, expected_attrs=expected_attrs,
            filters=filters, **page_args)
        count = yield client.object_user_get_count(ctxt, filters=filters)
        self.write(objects.json_encode({
            "object_users": object_users,
            "total": count
        }))

    @gen.coroutine
    def post(self):
        """Create object_user

        ---
        tags:
        - object_user
        summary: Create object_user
        description: Create object_user
        operationId: object_users.api.create_object_user
        produces:
        - application/json
        parameters:
        - in: body
          name: object_user
          description: Created volume object
          required: true
          schema:
            type: object
            properties:
              uid:
                type: string
                description: name
              description:
                type: string
                description: description
              email:
                type: string
                description: email
              display_name:
                type: string
                description: uid
              max_buckets:
                type: integer
                format: int32
                description: user_max_buckets
              bucket_quota_max_size:
                type: integer
                format: int32
                description: user_bucket_quota_max_size
              bucket_quota_max_objects:
                type: integer
                format: int32
                description: user_bucket_quota_max_objects
              user_quota_max_size:
                type: integer
                format: int32
                description: user_user_quota_max_size
              user_quota_max_objects:
                type: integer
                format: int32
                description: user_user_quota_max_objects
              op_mask:
                type: string
                description: user_op_mask
        responses:
        "200":
          description: successful operation
        """

        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=create_object_user_schema,
                 format_checker=draft7_format_checker)
        data = data.get("object_user")
        client = self.get_admin_client(ctxt)
        user = yield client.object_user_create(ctxt, data)
        self.write(objects.json_encode({
            "object_user": user
        }))


@URLRegistry.register(r"/object_users/([0-9]*)/")
class ObjectUserHandler(ClusterAPIHandler):

    @gen.coroutine
    def get(self, object_user_id):
        """ObjectUser Detail

        ---
        tags:
        - object_user
        summary: Detail of the object_user
        description: Return detail infomation of object_user by id
        operationId: object_users.api.object_user_detail
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
          description: ObjectUser ID
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
        expected_attrs = ['access_keys']
        object_user = yield client.object_user_get(
            ctxt, object_user_id, expected_attrs=expected_attrs)
        self.write(objects.json_encode({"object_user": object_user}))

    @gen.coroutine
    def delete(self, object_user_id):
        """Delete object_user

        ---
        tags:
        - object_user
        summary: delete object_user
        produces:
        - application/json
        parameters:
        - in: URL
          name: id
          description: object_user_id
          required: true
          schema:
            type: int
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        force_delete = self.get_paginated_args()
        object_user = yield client.object_user_delete(
            ctxt, object_user_id, force_delete)
        self.write(objects.json_encode({
            "object_user": object_user
        }))


@URLRegistry.register(r"/object_users/([0-9]*)/action/")
class ObjectUserActionHandler(ClusterAPIHandler):
    @gen.coroutine
    def put(self, object_user_id):
        """
        ---
        tags:
        - object_user role
        summary: Update object_user
        description: update object_user suspended
        operationId: object_user.api.updateObjectUser
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
          description: object_user ID
          schema:
            type: integer
            format: int32
          required: true
        - in: body
          name: node
          description: updated object_user role
          required: true
          schema:
            type: object
            properties:
              object_user:
                type: object
                properties:
                  spspended:
                    type: boolean
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=update_object_user_schema,
                 format_checker=draft7_format_checker)
        suspended = data.get('object_user')
        client = self.get_admin_client(ctxt)
        object_user = yield client.object_user_suspended_update(
            ctxt, object_user_id, suspended)
        self.write(objects.json_encode({
            "object_user": object_user
        }))
