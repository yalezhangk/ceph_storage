import json
import logging

from tornado import gen

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class ServiceListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        context = self.get_context()
        page_args = self.get_paginated_args()

        node_id = self.get_query_argument('node', default=None)
        filters = {}
        if node_id:
            filters.update({
                'node_id': node_id
            })

        client = self.get_admin_client(context)
        services = yield client.services_get_all(
            context, filters=filters, **page_args)

        self.write(objects.json_encode({
            "services": services
        }))


class ServiceHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        self.write(json.dumps({}))
