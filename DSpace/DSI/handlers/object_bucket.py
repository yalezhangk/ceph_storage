import logging

from jsonschema import draft7_format_checker
from jsonschema import validate
from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


create_object_bucket_schema = {
    "type": "object",
    "properties": {
        "bucket": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "minLength": 5,
                    "maxLength": 32
                },
                "owner_id": {"type": "integer"},
                "policy_id": {"type": "integer"},
                "versioned": {"type": "boolean"},
                "owner_permission": {
                    "type": "string",
                    "enum": ["FULL_CONTROL"]
                    },
                "auth_permission": {
                    "type": "string",
                    "enum": ["READ", "WRITE", "READ,WRITE", ""]
                    },
                "all_user_permission": {
                    "type": "string",
                    "enum": ["READ", "WRITE", "READ,WRITE", ""]
                    },
                "quota_max_size": {"type": "integer"},
                "quota_max_objects": {"type": "integer"}
            },
            "required": ["name", "owner_id", "policy_id"
                         "quota_max_size", "quota_max_objects"],
        },
    },
    "required": ["bucket"],
}


@URLRegistry.register(r"/object_buckets/")
class BucketListHandler(ClusterAPIHandler):

    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - bucket
        summary: bucket List
        description: Return a list of buckets
        operationId: bucket.api.listBucket
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
        pass

    @gen.coroutine
    def post(self):
        """Create bucket

        ---
        tags:
        - bucket
        summary: Create bucket
        description: Create bucket.
        operationId: object_policies.api.create_bucket
        produces:
        - application/json
        parameters:
        - in: body
          name: bucket
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
        validate(data, schema=create_object_bucket_schema,
                 format_checker=draft7_format_checker)
        data = data.get("bucket")
        client = self.get_admin_client(ctxt)
        bucket_list = yield client.object_buckets_create(ctxt, data)
        if data['batch_create']:
            self.write(objects.json_encode({
                "buckets": bucket_list
            }))
        else:
            self.write(objects.json_encode({
                "bucket": bucket_list[0]
                }))


@URLRegistry.register(r"/object_buckets/([0-9]*)/")
class BucketHandler(ClusterAPIHandler):

    @gen.coroutine
    def get(self, object_policy_id):
        """ObjectBucket Detail

        ---
        tags:
        - bucket
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
        pass

    @gen.coroutine
    def put(self, bucketid):
        """Update bucket

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
        pass

    @gen.coroutine
    def delete(self, bucket_id):
        """Delete object_bucket

        ---
        tags:
        - object_bucket
        summary: delete object_bucket
        produces:
        - application/json
        parameters:
        - in: URL
          name: id
          description: object_bucket id
          required: true
          schema:
            type: int
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        object_bucket = yield client.object_bucket_delete(
            ctxt, bucket_id)
        self.write(objects.json_encode({
            "object_bucket": object_bucket
        }))
