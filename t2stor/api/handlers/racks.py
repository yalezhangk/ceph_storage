#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class RackListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """获取机架列表信息
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        racks = yield client.rack_get_all(ctxt)
        self.write(objects.json_encode({
            "racks": racks
        }))

    # 创建机架
    # 传入参数：datacenter_id
    @gen.coroutine
    def post(self):
        """创建机架

        {"datacenter_id": 1}
        """
        datacenter_id = json_decode(self.request.body).get('datacenter_id')
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        rack = yield client.datacenter_create(ctxt, datacenter_id)
        logger.debug(
            "rack: id: {}, name: {}, datacenter_id, cluster_id: {}".format(
                rack.id, rack.name, rack.datacenter_id, rack.cluster_id))
        self.write(objects.json_encode({
            "racks": rack
        }))


class RackHandler(ClusterAPIHandler):
    # TODO 获取机架详情
    @gen.coroutine
    def get(self, rack_id):
        pass

    # TODO 修改机架名称
    @gen.coroutine
    def put(self, rack_id):
        pass

    # TODO 删除机架
    @gen.coroutine
    def delete(self, rack_id):
        pass


class RackHostsHandler(ClusterAPIHandler):
    # TODO 获取机架下的虚拟host
    @gen.coroutine
    def get(self, rack_id):
        pass

    # TODO 在机架下创建虚拟主机
    @gen.coroutine
    def post(self, rack_id):
        pass
