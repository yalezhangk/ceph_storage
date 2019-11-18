import json
import logging

from tornado import gen

from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


@URLRegistry.register(r"/services/")
class ServiceListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - service
        summary: return a list of services
        description: return a list of services
        operationId: services.api.listService
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
          name: node
          description: Node ID
          type: string
          required: false
        responses:
        "200":
          description: successful operation
        """
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
        service_count = yield client.service_get_count(
            context, filters=filters)

        self.write(objects.json_encode({
            "services": services,
            "total": service_count
        }))


class ServiceHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        self.write(json.dumps({}))
