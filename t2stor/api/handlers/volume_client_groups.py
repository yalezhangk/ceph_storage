import logging

from tornado import gen

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class VolumeClientGroupHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        volume_client_groups = yield client.volume_client_group_get_all(ctxt)
        self.write(objects.json_encode({
            "volume_client_group": volume_client_groups
        }))
