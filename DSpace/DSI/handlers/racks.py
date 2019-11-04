#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers.base import ClusterAPIHandler

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

    @gen.coroutine
    def post(self):
        """创建机架

        {"datacenter_id": 1}
        """
        datacenter_id = json_decode(self.request.body).get('datacenter_id')
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        rack = yield client.rack_create(ctxt, datacenter_id)
        logger.debug(
            "rack: id: {}, name: {}, datacenter_id, cluster_id: {}".format(
                rack.id, rack.name, rack.datacenter_id, rack.cluster_id))
        self.write(objects.json_encode({
            "rack": rack
        }))


class RackHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, rack_id):
        """获取机架信息
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        rack = yield client.rack_get(ctxt, rack_id)
        self.write(objects.json_encode({
            "rack": rack
        }))

    @gen.coroutine
    def put(self, rack_id):
        """修改机架信息：机架名称或所属的数据中心

        {"rack": {"name":"rack-name"}}
        {"rack": {"datacenter_id": 1}}
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        data = json_decode(self.request.body).get('rack')
        rack_name = data.get('name')
        datacenter_id = data.get('datacenter_id')
        if rack_name:
            rack = yield client.rack_update_name(
                ctxt, rack_id, rack_name)
        if datacenter_id:
            rack = yield client.rack_update_toplogy(
                ctxt, rack_id, datacenter_id)
        logger.debug("rack: id: {}, name: {}, datacenter_id: {}".format(
            rack.id, rack.name, rack.datacenter_id
        ))
        self.write(objects.json_encode({
            "rack": rack
        }))

    @gen.coroutine
    def delete(self, rack_id):
        """删除机架
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        rack = yield client.rack_delete(ctxt, rack_id)
        self.write(objects.json_encode({
            "rack": rack
        }))


class RackHostsHandler(ClusterAPIHandler):
    # TODO 获取机架下的虚拟host
    @gen.coroutine
    def get(self, rack_id):
        pass
