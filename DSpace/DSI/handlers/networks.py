import json
import logging

from tornado import gen

from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


@URLRegistry.register(r"/networks/")
class NetworkListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - network
        summary: network List
        description: Return a list of networks
        operationId: networks.api.listNetwork
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
        - in: request
          name: node_id
          description: Find all networks on the node by ID
          type: int
          format: int32
          required: false
        responses:
        "200":
          description: successful operation
        """
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


@URLRegistry.register(r"/networks/")
class NetworkHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        self.write(json.dumps({}))
