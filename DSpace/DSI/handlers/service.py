import json
import logging

from tornado import gen

from DSpace import objects
from DSpace.DSI.handlers.base import ClusterAPIHandler

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
        services_all = yield client.services_get_all(
            context, filters=filters)

        self.write(objects.json_encode({
            "services": services,
            "total": len(services_all),
        }))


class ServiceHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        self.write(json.dumps({}))