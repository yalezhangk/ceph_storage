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

create_object_user_key_schema = {
    "type": "object",
    "properties": {
        "object_user": {
            "type": "object",
            "properties": {
                "access_key": {"type": "string"},
                "secret_key": {"type": "string"}
            }
        },
    },
    "required": ["object_user"],
}

update_object_user_key_schema = {
    "type": "object",
    "properties": {
        "object_user_key": {
            "type": "object",
            "properties": {
                "object_user_key": {
                    "type": "string"
                },
            },
        },
    },
    "required": ["object_user_key"],
}

update_object_user_email_schema = {
    "type": "object",
    "properties": {
        "object_user_email": {
            "type": "object",
            "properties": {
                "email":
                    {"type": ["string", "null"], "format": "email"}
            },
        },
    },
    "required": ["object_user_email"],
}

update_object_user_op_mask_schema = {
    "type": "object",
    "properties": {
        "object_user_op_mask": {
            "type": "object",
            "properties": {
                "op_mask": {
                    "type": "string"
                },
            },
        },
    },
    "required": ["object_user_op_mask"],
}

update_object_user_quota_schema = {
    "type": "object",
    "properties": {
        "object_user_quota": {
            "type": "object",
            "properties": {
                "user_quota": {
                    "type": "string"
                },
            },
        },
    },
    "required": ["object_user_quota"],
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
        tab = self.get_query_argument('tab', default=None)
        object_users = yield client.object_user_get_all(
            ctxt, expected_attrs=expected_attrs,
            filters=filters, tab=tab, **page_args)
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


@URLRegistry.register(r"/object_users/([0-9]*)/capacity/")
class ObjectUserCapacityHandler(ClusterAPIHandler):

    @gen.coroutine
    def get(self, object_user_id):
        """ObjectUser Capacity

        ---
        tags:
        - object_user
        summary: Capacity of the object_user
        description: Return capacity infomation of object_user by id
        operationId: object_users.api.object_user_capacity
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
        object_user = yield client.object_user_get_capacity(
            ctxt, object_user_id)
        self.write(objects.json_encode({"object_user": object_user}))


@URLRegistry.register(r"/object_user/([0-9]*)/key/")
class ObjectUserKeyHandler(ClusterAPIHandler):
    @gen.coroutine
    def post(self, object_user_id):
        """Create object_user_key

        ---
        tags:
        - object_user_key
        summary: Create object_user_key
        description: Create object_user_key
        operationId: object_users.api.create_object_user_key
        produces:
        - application/json
        parameters:
        - in: body
          name: object_user_key
          description: Created object_user_key
          required: true
          schema:
            type: object
            properties:
              access_key:
                type: string
                description: access_key
              secret_key:
                type: string
                description: secret_key
        responses:
        "200":
          description: successful operation
        """

        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=create_object_user_key_schema,
                 format_checker=draft7_format_checker)
        data = data.get("object_user")
        client = self.get_admin_client(ctxt)
        user = yield client.object_user_key_create(ctxt, data,
                                                   object_user_id)
        self.write(objects.json_encode({
            "object_user": user
        }))

    @gen.coroutine
    def delete(self, object_user_key_id):
        """Delete object_user

        ---
        tags:
        - object_user_key
        summary: delete object_user_key
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
        object_user_key = yield client.object_user_key_delete(
            ctxt, object_user_key_id)
        self.write(objects.json_encode({
            "object_user_key": object_user_key
        }))

    @gen.coroutine
    def put(self, object_user_key_id):
        """Update ObjectUserKey

        ---
        tags:
        - object_user_key
        summary: update object_user_key
        description: update object_user_key
        produces:
        - application/json
        parameters:
        - in: body
          name: object_user_key
          object_user_key: update object_user_key
          required: true
          schema:
            type: object
            properties:
              object_user_key:
                type: string
                object_user_key: new object_user_key
        responses:
        "200":
          description: successful operation
        """
        # 修改密钥对
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=update_object_user_key_schema,
                 format_checker=draft7_format_checker)
        object_user_key_data = data.get('object_user_key')
        client = self.get_admin_client(ctxt)
        object_user_key = yield client.object_user_key_update(
            ctxt, object_user_key_id, object_user_key_data)
        self.write(objects.json_encode({
            "object_user_key": object_user_key
        }))


@URLRegistry.register(r"/object_user/([0-9]*)/email/")
class ObjectUserEmailHandler(ClusterAPIHandler):
    @gen.coroutine
    def put(self, object_user_id):
        """Update ObjectUser

        ---
        tags:
        - object_user
        summary: update object_user
        description: update object_user email
        produces:
        - application/json
        parameters:
        - in: body
          name: object_user
          description: update object_user email
          required: true
          schema:
            type: object
            properties:
              email:
                type: string
                email: new email
        responses:
        "200":
          description: successful operation
        """
        # 更改邮箱
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=update_object_user_email_schema,
                 format_checker=draft7_format_checker)
        object_user_data = data.get('object_user_email')
        client = self.get_admin_client(ctxt)
        object_user = yield client.object_user_email_update(
            ctxt, object_user_id, object_user_data)
        self.write(objects.json_encode({
            "object_user": object_user
        }))


@URLRegistry.register(r"/object_users/([0-9]*)/op_mask/")
class ObjectUserOpMaskHandler(ClusterAPIHandler):
    @gen.coroutine
    def put(self, object_user_id):
        """
        ---
        tags:
        - object_user op_mask
        summary: Update object_user op_mask
        description: update object_user op_mask
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
          description: updated object_user op_mask
          required: true
          schema:
            type: object
            properties:
              object_user:
                type: object
                properties:
                  op_mask:
                    type: string
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=update_object_user_op_mask_schema,
                 format_checker=draft7_format_checker)
        op_mask = data.get('object_user_op_mask')
        client = self.get_admin_client(ctxt)
        object_user = yield client.object_user_set_op_mask(
            ctxt, object_user_id, op_mask)
        self.write(objects.json_encode({
            "object_user": object_user
        }))


@URLRegistry.register(r"/object_users/([0-9]*)/user_quota/")
class ObjectUserquotaHandler(ClusterAPIHandler):
    @gen.coroutine
    def put(self, object_user_id):
        """
        ---
        tags:
        - object_user quota
        summary: Update object_user quota
        description: update object_user quota
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
          description: updated object_user quota
          required: true
          schema:
            type: object
            properties:
              object_user:
                type: object
                properties:
                  quota:
                    type: string
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=update_object_user_quota_schema,
                 format_checker=draft7_format_checker)
        user_quota = data.get('object_user_quota')
        client = self.get_admin_client(ctxt)
        object_user = yield client.object_user_set_user_quota(
            ctxt, object_user_id, user_quota)
        self.write(objects.json_encode({
            "object_user": object_user
        }))


@URLRegistry.register(r"/object_users/([0-9]*)/metrics/")
class ObjectUserMetricsHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, object_user_id):
        """
        ---
        tags:
        - object_user
        summary: object_user's Metrics
        description: return the Metrics of object_user by id
        operationId: object_user.api.getMetrics
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
          description: object_user's id
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
        data = yield client.object_user_metrics_get(
            ctxt, object_user_id=object_user_id)
        self.write(objects.json_encode({
            "object_user_metrics": data
        }))


@URLRegistry.register(r"/object_users/([0-9]*)/history_metrics/")
class ObjectUserMetricsHistoryHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, object_user_id):
        """
        ---
        tags:
        - object_user
        summary: object_user's History Metrics
        description: return the History Metrics of object_user by id
        operationId: object_users.api.getHistoryMetrics
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
          description: object_user's id
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
        data = yield client.object_user_metrics_history_get(
            ctxt, obj_user_id=object_user_id, start=his_args['start'],
            end=his_args['end'])
        self.write(objects.json_encode({
            "object_user_history_metrics": data
        }))
