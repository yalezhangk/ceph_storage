import logging

from jsonschema import draft7_format_checker
from jsonschema import validate
from oslo_utils import strutils
from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler
from DSpace.exception import InvalidInput
from DSpace.i18n import _

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
                    "enum": ["READ", "WRITE", "READ,WRITE",
                             "WRITE,READ", ""]
                    },
                "all_user_permission": {
                    "type": "string",
                    "enum": ["READ", "WRITE", "READ,WRITE",
                             "WRITE,READ", ""]
                    },
                "quota_max_size": {"type": "integer"},
                "quota_max_objects": {"type": "integer"}
            },
            "required": ["name", "owner_id", "policy_id",
                         "quota_max_size", "quota_max_objects"]
        }
    },
    "required": ["bucket"]
}

update_object_bucket_quota_schema = {
    "type": "object",
    "properties": {
        "bucket": {
            "type": "object",
            "properties": {
                "quota_max_size": {"type": "integer"},
                "quota_max_objects": {"type": "integer"}
            },
            "required": ["quota_max_size", "quota_max_objects"]
        }
    },
    "required": ["bucket"]
}

update_object_bucket_owner_schema = {
    "type": "object",
    "properties": {
        "bucket": {
            "type": "object",
            "properties": {
                "owner_id": {"type": "integer"}
            },
            "required": ["owner_id"]
        }
    },
    "required": ["bucket"]
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
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        page_args = self.get_paginated_args()
        expected_attrs = ['policy', 'owner', 'lifecycles']
        fuzzy_filters = ['name']
        filters = self.get_support_filters(fuzzy_filters=fuzzy_filters)
        tab = self.get_query_argument('tab', default=None)
        object_user_id = self.get_query_argument(
                'object_user_id', default=None)
        if tab not in [None, "default", "io"]:
            raise InvalidInput(_("this tab is not supported"))
        if object_user_id is None:
            pass
        elif object_user_id.isdigit():
            filters['owner_id'] = object_user_id
        else:
            raise InvalidInput(_(
                "The object_user_id ({}) is invalid").format(object_user_id))
        buckets = yield client.bucket_get_all(
                ctxt, tab, expected_attrs=expected_attrs,
                filters=filters, **page_args)
        bucket_count = yield client.bucket_get_count(ctxt, filters)
        self.write(objects.json_encode({
            "buckets": buckets,
            "total": bucket_count
            }))

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
    def get(self, bucket_id):
        """ObjectBucket Detail

        ---
        tags:
        - bucket
        summary: Detail of the object_bucket
        description: Return detail infomation of object_bucket by id
        operationId: object_buckets.api.object_bucket_detail
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
          description: ObjectBucket ID
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
        expected_attrs = ['owner', 'policy', 'lifecycles']
        bucket = yield client.bucket_get(
            ctxt, bucket_id, expected_attrs=expected_attrs)
        self.write(objects.json_encode({"bucket": bucket}))

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
        logger.info("trying to delete bucket: %s", bucket_id)
        client = self.get_admin_client(ctxt)
        force = self.get_query_argument('force', default='False')
        if force not in ["False", "True"]:
            raise InvalidInput(_("force supports only True and False"))
        force = strutils.bool_from_string(force)
        bucket = yield client.object_bucket_delete(
            ctxt, bucket_id, force)
        logger.info("delete bucket: %s success", bucket_id)
        self.write(objects.json_encode({
            "bucket": bucket
        }))


@URLRegistry.register(r"/object_buckets/([0-9]*)/capacity/")
class ObjectBucketCapacityHandler(ClusterAPIHandler):

    @gen.coroutine
    def get(self, bucket_id):
        """ObjectBucket Capacity

        ---
        tags:
        - object_bucket
        summary: Capacity of the object_bucket
        description: Return capacity infomation of object_bucket by id
        operationId: object_buckets.api.object_bucket_capacity
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
          description: ObjectBucket ID
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
        bucket_capacity = yield client.bucket_get_capacity(
            ctxt, bucket_id)
        self.write(objects.json_encode({"bucket_capacity": bucket_capacity}))


@URLRegistry.register(r"/object_buckets/([0-9]*)/action/")
class ObjectBucketActionHandler(ClusterAPIHandler):

    @gen.coroutine
    def _bucket_quota(self, client, ctxt, bucket_id, bucket_data):
        quota = yield client.bucket_update_quota(ctxt, bucket_id, bucket_data)
        return quota

    @gen.coroutine
    def _bucket_owner(self, client, ctxt, bucket_id, bucket_data):
        owner = yield client.bucket_update_owner(ctxt, bucket_id, bucket_data)
        return owner

    @gen.coroutine
    def _set_access_control(self, client, ctxt, bucket_id, bucket_data):
        access_control = yield client.bucket_update_access_control(
                ctxt, bucket_id, bucket_data)
        return access_control

    @gen.coroutine
    def _versioning(self, client, ctxt, bucket_id, bucket_data):
        versioning = yield client.bucket_versioning_update(
                ctxt, bucket_id, bucket_data)
        return versioning

    @gen.coroutine
    def put(self, bucket_id):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        bucket_data = data.get('bucket')
        action = data.get('action')
        client = self.get_admin_client(ctxt)
        map_action = {
            'quota': self._bucket_quota,
            'owner': self._bucket_owner,
            'versioning': self._versioning,
            'access_control': self._set_access_control
        }
        fun_action = map_action.get(action)
        if fun_action is None:
            raise exception.ObjectBucketActionNotFound(action=action)
        result = yield fun_action(client, ctxt, bucket_id, bucket_data)
        self.write(objects.json_encode({
            "bucket": result
        }))


@URLRegistry.register(r"/object_buckets/([0-9]*)/metrics/")
class ObjectBucketMetricsHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, object_bucket_id):
        """
        ---
        tags:
        - object_bucket
        summary: object_bucket's Metrics
        description: return the Metrics of object_bucket by id
        operationId: object_bucket.api.getMetrics
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
          description: object_bucket's id
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
        data = yield client.object_bucket_metrics_get(
            ctxt, object_bucket_id=object_bucket_id)
        self.write(objects.json_encode({
            "object_bucket_metrics": data
        }))


@URLRegistry.register(r"/object_buckets/([0-9]*)/history_metrics/")
class ObjectBucketMetricsHistoryHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, object_bucket_id):
        """
        ---
        tags:
        - object_bucket
        summary: object_bucket's History Metrics
        description: return the History Metrics of object_bucket by id
        operationId: object_bucket.api.getHistoryMetrics
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
          description: object_bucket's id
          schema:
            type: integer
            format: int32
          required: true
        - in: request
          name: start
          description: the start of the history, it must be a time stamp.
                       eg.1573600118.935
          schema:
            type: integer
            format: int32
          required: true
        - in: request
          name: end
          description: the end of the history, it must be a time stamp.
                       eg.1573600118.936
          schema:
            type: integer
            format: int32
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        his_args = self.get_metrics_history_args()
        client = self.get_admin_client(ctxt)
        data = yield client.object_bucket_metrics_history_get(
            ctxt, object_bucket_id=object_bucket_id, start=his_args['start'],
            end=his_args['end'])
        self.write(objects.json_encode({
            "object_bucket_history_metrics": data
        }))
