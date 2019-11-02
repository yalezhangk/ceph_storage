import json
import logging

from tornado import gen

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class NetworkListHandler(ClusterAPIHandler):
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
        page_args.update({"filters": filters})

        client = self.get_admin_client(context)
        networks = yield client.network_get_all(
            context, expected_attrs=['node'], **page_args)
        networks_all = yield client.network_get_all(
            context, expected_attrs=['node'])

        self.write(objects.json_encode({
            "networks": networks,
            "total": len(networks_all)
        }))


class NetworkHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        self.write(json.dumps({}))
