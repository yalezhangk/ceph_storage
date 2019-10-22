#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from tornado import gen
from tornado.escape import json_decode

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler
from t2stor.exception import InvalidInput
from t2stor.i18n import _

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

        {}
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
    # TODO 获取数据中心详情
    @gen.coroutine
    def get(self, datacenter_id):
        pass

    @gen.coroutine
    def put(self, datacenter_id):
        """修改数据中心名称

        {"datacenter": {"name":"datacenter-name"}}
        """
        # 获取前段传递的所属数据中心的名称
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

    # TODO 删除数据中心
    @gen.coroutine
    def delete(self, datacenter_id):
        pass


class DataCenterRacksHandler(ClusterAPIHandler):
    # TODO 获取数据中心下的机架
    @gen.coroutine
    def get(self, datacenter_id):
        pass

    # TODO 在数据中心下创建机架
    @gen.coroutine
    def post(self, datacenter_id):
        pass
