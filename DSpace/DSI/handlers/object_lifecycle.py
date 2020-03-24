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


modify_object_lifecycles_schema = {
    "definitions": {
        "per_lifecycle": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "enabled": {"type": "boolean"},
                "target": {
                    "type": "string",
                    "minLength": 0,
                    "maxLength": 128
                },
                "policy": {
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": ["delete"]
                        },
                        "expiration": {"type": ["integer", "string"]},
                        "mode": {
                            "type": "string",
                            "enum": ["days", "date"]
                        },

                    },
                    "required": ["type", "expiration", "mode"]
                },
            },
            "required": ["name", "enabled", "target", "policy"],
        },
    },
    "type": "object",
    "properties": {
        "object_lifecycle": {
            "type": "object",
            "properties": {
                "bucket_id": {"type": "integer"},
                "object_lifecycles": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "$ref": "#/definitions/per_lifecycle"},
                },
            },
            "required": ["bucket_id", "object_lifecycles"],
        },
    },
    "required": ["object_lifecycle"],
}

lifecycles_update_execute_time_schema = {
    "type": "object",
    "properties": {
        "object_lifecycle": {
            "type": "object",
            "properties": {
                "start_on": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 64
                },
                "end_on": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 64
                },
            },
            "required": ["start_on", "end_on"],
        },
    },
    "required": ["object_lifecycle"],
}


@URLRegistry.register(r"/object_lifecycles/")
class ObjectLifecycleListHandler(ClusterAPIHandler):

    @gen.coroutine
    def get(self):
        """object_lifecycle List by bucket_id

        ---
        tags:
        - object_lifecycle
        summary: object_lifecycle List by bucket_id
        description: Return a list of object_lifecycle
        operationId: object_lifecycle.api.listObjectLifecycle
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
          name: bucket_id
          description: bucket ID
          schema:
            type: integer
            format: int32
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
        bucket_id = self.get_query_argument('bucket_id', default=None)
        if not bucket_id:
            raise exception.Invalid(_('missing query_argument bucket_id'))

        expected_attrs = ['bucket']
        joined_load = self.get_query_argument('joined_load', default=None)
        if joined_load == '0':
            expected_attrs = []
        fuzzy_filters = ['name']
        filters = self.get_support_filters(fuzzy_filters=fuzzy_filters)
        filters['bucket_id'] = int(bucket_id)

        lifecycles = yield client.object_lifecycle_get_all(
            ctxt, expected_attrs=expected_attrs, filters=filters, **page_args)
        count = yield client.object_lifecycle_get_count(ctxt, filters=filters)
        self.write(objects.json_encode({
            "object_lifecycles": lifecycles,
            "total": count
        }))

    @gen.coroutine
    def put(self):
        """Create or Modify ObjectLifecycles for a bucket

        ---
        tags:
        - object_lifecycles
        summary: Create or Modify object_lifecycles
        description: Create or Modify object_lifecycles.
        operationId: object_policies.api.modify_object_lifecycles
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
              bucket_id:
                type: integer
                description: bucket_id
              object_lifecycles:
                type: array
                description: a group object_lifecycle
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=modify_object_lifecycles_schema,
                 format_checker=draft7_format_checker)
        data = data.get("object_lifecycle")
        client = self.get_admin_client(ctxt)
        lifecycles = yield client.object_lifecycle_modify(ctxt, data)
        self.write(objects.json_encode({
            "object_lifecycle": lifecycles
                                }))


@URLRegistry.register(r"/object_lifecycles/execute_time/")
class ObjectLifecycleHandler(ClusterAPIHandler):

    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        execute_time = yield client.object_lifecycle_get_execute_time(ctxt)
        self.write(objects.json_encode({
            "execute_time": execute_time,
        }))

    @gen.coroutine
    def put(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        data = json_decode(self.request.body)
        validate(data, schema=lifecycles_update_execute_time_schema,
                 format_checker=draft7_format_checker)
        data = data.get("object_lifecycle")
        lifecycles = yield client.object_lifecycle_update_execute_time(
            ctxt, data)
        self.write(objects.json_encode({
            "object_lifecycle": lifecycles
        }))
