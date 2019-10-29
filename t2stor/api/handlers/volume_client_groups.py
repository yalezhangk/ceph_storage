import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class VolumeClientGroupListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        volume_client_groups = yield client.volume_client_group_get_all(ctxt)
        self.write(objects.json_encode({
            "volume_client_group": volume_client_groups
        }))

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)

        volume_client_group = json_decode(self.request.body)
        volume_clients = volume_client_group.pop("clients")

        volume_client_group = \
            yield client.volume_client_group_create(ctxt, volume_client_group)
        for volume_client in volume_clients:
            volume_client["volume_client_group_id"] = volume_client_group.id
            yield client.volume_client_create(ctxt, volume_client)
        self.write(objects.json_encode({
            "volume_client_group": volume_client_group
        }))


class VolumeClientGroupHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, group_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        volume_client_group = yield client.volume_client_group_get(
            ctxt, group_id)
        self.write(objects.json_encode(
            {"volume_client_group": volume_client_group}))

    @gen.coroutine
    def delete(self, group_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        volume_client_group = yield client.volume_client_group_delete(
            ctxt, group_id)
        self.write(objects.json_encode(
            {"volume_client_group": volume_client_group}))


class VolumeClientByGroup(ClusterAPIHandler):
    @gen.coroutine
    def get(self, group_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        volume_clients = yield client.volume_client_get_by_group(
            ctxt, group_id)
        self.write(objects.json_encode({"volume_clients": volume_clients}))
