import logging

from tornado import gen

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class PoolListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        pools = yield client.pool_get_all(ctxt)
        self.write(objects.json_encode({
            "pool": pools
        }))


class PoolHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, pool_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        pool = yield client.pool_get(ctxt, pool_id)
        self.write(objects.json_encode({"pool": pool}))


class PoolOsdsHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, pool_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        osds = yield client.pool_osds_get(ctxt, pool_id)
        self.write(objects.json_encode({"osds": osds}))
