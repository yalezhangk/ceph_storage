import json
import logging

from tornado import gen

from DSpace import objects
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class NetworkListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        page_args = self.get_paginated_args()

        node_id = self.get_query_argument('node', default=None)
        filters = {}
        if node_id:
            filters.update({
                'node_id': node_id
            })
        page_args.update({"filters": filters})

        client = self.get_admin_client(ctxt)
        networks = yield client.network_get_all(
            ctxt, expected_attrs=['node'], **page_args)
        network_count = yield client.network_get_count(ctxt)

        self.write(objects.json_encode({
            "networks": networks,
            "total": network_count
        }))


class NetworkHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        self.write(json.dumps({}))
