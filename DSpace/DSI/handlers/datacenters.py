#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers.base import ClusterAPIHandler
from DSpace.exception import InvalidInput
from DSpace.i18n import _

logger = logging.getLogger(__name__)


class DataCenterListHandler(ClusterAPIHandler):
    # TODO 返回数据中心中的rack和vhost
    @gen.coroutine
    def get(self):
        """数据中心列表
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        datacenters = yield client.datacenter_get_all(ctxt)
        self.write(objects.json_encode({
            "datacenters": datacenters
        }))

    @gen.coroutine
    def post(self):
        """创建数据中心
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        datacenter = yield client.datacenter_create(ctxt)
        logger.error("datacenter: id:{}, name: {}, cluster_id: {}".format(
            datacenter.id, datacenter.name, datacenter.cluster_id
        ))
        self.write(objects.json_encode({
            "datacenter": datacenter
        }))


class DataCenterHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, datacenter_id):
        """获取数据中心详情
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        datacenter = yield client.datacenter_get(ctxt, datacenter_id)
        self.write(objects.json_encode({
            "datacenter": datacenter
        }))

    @gen.coroutine
    def put(self, datacenter_id):
        """修改数据中心名称

        {"datacenter": {"name":"datacenter-name"}}
        """
        data = json_decode(self.request.body).get('datacenter')
        datacenter_name = data.get('name')
        if not datacenter_name:
            raise InvalidInput(reason=_("datacenter: name is none"))
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        datacenter = yield client.datacenter_update(
            ctxt, datacenter_id, datacenter_name)
        logger.debug("datacenter: id: {}, name: {}, cluster_id: {}".format(
            datacenter.id, datacenter.name, datacenter.cluster_id
        ))
        self.write(objects.json_encode({
            "datacenter": datacenter
        }))

    @gen.coroutine
    def delete(self, datacenter_id):
        """删除数据中心
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        datacenter = yield client.datacenter_delete(ctxt, datacenter_id)
        self.write(objects.json_encode({
            "datacenter": datacenter
        }))


class DataCenterRacksHandler(ClusterAPIHandler):
    # TODO 获取数据中心下的机架
    @gen.coroutine
    def get(self, datacenter_id):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        racks = yield client.datacenter_racks(ctxt, datacenter_id)
        self.write(objects.json_encode({
            "racks": racks
        }))
