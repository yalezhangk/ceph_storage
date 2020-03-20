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
