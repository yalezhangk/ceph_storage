import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.db.sqlalchemy import api
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


@URLRegistry.register(r"/configssl/")
class ServiceListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        context = self.get_context()
        ssls = api.configssl_get_all(context)
        data = [{
            "id": ssl.id,
            "name": ssl.name,
            "domain_name": "t2cloud.net",
            "not_before": "2020-03-15",
            "not_after": "2021-03-16",
        } for ssl in ssls]

        self.write(objects.json_encode({
            "ssls": data,
            "total": len(data)
        }))

    @gen.coroutine
    def post(self):
        context = self.get_context()
        data = json_decode(self.request.body)
        configssl = api.configssl_create(context, {
            "name": data['ssl'].get("name"),
            "crt": data['ssl'].get("crt"),
            "key": data['ssl'].get("key")
        })
        self.write(objects.json_encode({
            "ssl": {
                "id": 0,
                "name": configssl.name,
                "domain_name": configssl.domain_name,
                "not_before": configssl.not_before,
                "not_after": configssl.not_before,
            }
        }))
