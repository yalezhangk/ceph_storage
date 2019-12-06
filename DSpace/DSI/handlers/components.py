import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


@URLRegistry.register(r"/components/")
class ComponentHandler(ClusterAPIHandler):

    @gen.coroutine
    def get(self):
        """
        查询所有的 mon、osd、mgr、rgw, mds
        :return: {
            mon:{}, mgr: {}, osd: {}, rgw: {}, mds: {}
        }

        ---
        tags:
        - components
        summary: return the ceph components
        description: return the ceph components
        operationId: components.api.getComponents
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
          name: services
          description: which services you want to restart,it can be
                       mon|mgr|osd|rgw|all|mds.
          schema:
            type: string
        responses:
        "200":
          description: successful operation

        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        service = self.get_query_argument('services', default=None)
        services = [service] if service else None
        components = yield client.components_get_list(ctxt, services)
        self.write(objects.json_encode(components))


@URLRegistry.register(r"/components/restart")
class ComponentRestartHandler(ClusterAPIHandler):
    @gen.coroutine
    def post(self):
        """重启
        {
            "component": {
                "service": "mon|mds|osd|mgr|rgw",
                "id": 1
            }
        }
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        data = json_decode(self.request.body)
        component = data.get('component')
        result = yield client.component_restart(ctxt, component)
        self.write(objects.json_encode({"res": result}))
