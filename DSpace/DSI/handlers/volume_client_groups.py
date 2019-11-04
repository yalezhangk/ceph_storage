import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import exception
from DSpace import objects
from DSpace.DSI.handlers.base import ClusterAPIHandler
from DSpace.i18n import _
from DSpace.utils.normalize_wwn import normalize_wwn

logger = logging.getLogger(__name__)


class VolumeClientGroupListHandler(ClusterAPIHandler):
    @gen.coroutine
    def check_volume_client_group(self, ctxt, client, volume_client_group):
        """verify that the volume cilent group's name exists."""
        name = volume_client_group.get('name')
        filters = {"name": name}
        vcg = yield client.volume_client_group_get_all(ctxt, filters=filters)
        if vcg:
            logger.error("Im raise a exception")
            raise exception.VolumeClientGroupExists(
                volume_client_group_name=name)

    @gen.coroutine
    def check_volume_client(self, ctxt, client, volume_client):
        """verify that the volume cilent's name exists."""
        iqn = volume_client.get('iqn')
        filters = {"iqn": iqn}
        if iqn:
            normalize_iqn, wwn_type = normalize_wwn(iqn)
            if not normalize_iqn:
                raise exception.Invalid(
                    _("iqn: {} is not allowed!").format(iqn))

            vc = yield client.volume_client_get_all(ctxt, filters=filters)
            if vc:
                raise exception.VolumeClientExists(iqn=iqn)

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

        # 验证
        # check方法返回的是Futrue，所以这里还需要一个yield来推动
        yield self.check_volume_client_group(ctxt, client, volume_client_group)
        for volume_client in volume_clients:
            yield self.check_volume_client(ctxt, client, volume_client)
        # 创建
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
        filters = {"volume_client_group_id": group_id}
        volume_clients = yield client.volume_client_get_all(
            ctxt, filters=filters)
        self.write(objects.json_encode({"volume_clients": volume_clients}))
