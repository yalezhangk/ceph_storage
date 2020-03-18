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
    def post(self):
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
