import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import exception
from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler
from t2stor.i18n import _

logger = logging.getLogger(__name__)


class CheckVolumeAccessPath():
    @gen.coroutine
    def check_volume_access_path_by_id(self, ctxt, client, id):
        v = yield client.volume_access_path_get(ctxt, id)
        if not v:
            raise exception.VolumeAccessPathNotFound(access_path_id=id)

    @gen.coroutine
    def check_volume_access_path_by_name(self, ctxt, client, name):
        filters = {"name": name}
        v = yield client.volume_access_path_get_all(ctxt, filters=filters)
        if v:
            raise exception.Duplicate(
                _("volume_access_path: {} is already exists!").format(name))


class VolumeAccessPathListHandler(ClusterAPIHandler, CheckVolumeAccessPath):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        volume_access_paths = yield client.volume_access_path_get_all(ctxt)
        self.write(objects.json_encode({
            "volume_access_paths": volume_access_paths
        }))

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)

        # TODO 数据验证 传没传 -> 有没有
        volume_access_path = json_decode(self.request.body)
        name = volume_access_path.get("name")
        type = volume_access_path.get("type")
        if not name or not type:
            raise exception.InvalidInput(_("name or type not found"))
        yield self.check_volume_access_path_by_name(ctxt, client, name)
        volume_access_path = yield client.volume_access_path_create(
            ctxt, volume_access_path)
        self.write(objects.json_encode({
            "volume_access_path": volume_access_path
        }))


class VolumeAccessPathHandler(ClusterAPIHandler, CheckVolumeAccessPath):
    @gen.coroutine
    def get(self, voluem_access_path_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        volume_access_path = yield client.volume_access_path_get(
            ctxt, voluem_access_path_id)
        self.write(objects.json_encode({
            "volume_access_path": volume_access_path
        }))

    @gen.coroutine
    def put(self, id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)

        # TODO 数据验证
        yield self.check_volume_access_path_by_id(ctxt, client, id)

        data = json_decode(self.request.body)
        if not data.get('name'):
            raise exception.InvalidInput(_("name or type not found"))
        yield self.check_volume_access_path_by_name(
            ctxt, client, data.get('name'))

        volume_access_path = yield client.volume_access_path_update(
            ctxt, id, data)
        self.write(objects.json_encode({
            "volume_access_path": volume_access_path
        }))

    @gen.coroutine
    def delete(self, id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        yield self.check_volume_access_path_by_id(ctxt, client, id)
        volume_access_path = yield client.volume_access_path_delete(ctxt, id)
        self.write(objects.json_encode({
            "volume_access_path": volume_access_path
        }))