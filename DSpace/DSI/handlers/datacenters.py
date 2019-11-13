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
        ---
        tags:
        - datacenter
        summary: Datacenter List
        description: Return a list of datacenters
        operationId: datacenters.api.listDatacenter
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
          name: limit
          description: Limit objects of response
          schema:
            type: integer
            format: int32
          required: false
        - in: request
          name: offset
          description: Skip objects of response
          schema:
            type: integer
            format: int32
          required: false
        responses:
        "200":
          description: successful operation
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
        ---
        tags:
        - datacenter
        summary: Create datacenter
        description: Create datacenter.
        operationId: datacenters.api.createDatacenter
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        responses:
        "200":
          description: successful operation
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
        ---
        tags:
        - datacenter
        summary: Detail of the datacenter
        description: Return detail infomation of datacenter by id
        operationId: datacenters.api.datacenterDetail
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: url
          name: id
          description: Datacenter ID
          schema:
            type: integer
            format: int32
          required: true
        responses:
        "200":
          description: successful operation
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

        ---
        tags:
        - datacenter
        summary: Update datacenter
        description: update datacenter.
        operationId: datacenters.api.updateDatacenter
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: url
          name: id
          description: Datacenter ID
          schema:
            type: integer
            format: int32
          required: true
        - in: body
          name: datacenter
          description: updated datacenter object
          required: true
          schema:
            type: object
            properties:
              datacenter:
                type: object
                properties:
                  name:
                    type: string
                    description: datacenter's name
        responses:
        "200":
          description: successful operation
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
        ---
        tags:
        - datacenter
        summary: Delete the datacenter by id
        description: delete datacenter by id
        operationId: datacenters.api.deleteDatacenter
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: url
          name: id
          description: Datacenter's id
          schema:
            type: integer
            format: int32
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        datacenter = yield client.datacenter_delete(ctxt, datacenter_id)
        self.write(objects.json_encode({
            "datacenter": datacenter
        }))


class DataCenterTreeHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - datacenter
        summary: Return datacenter tree
        description: return the tree of datacenter,
                     which have all of the datacenters, racks and nodes
        operationId: datacenters.api.getDatacenterTree
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        dc_tree = yield client.datacenter_tree(ctxt)
        self.write(objects.json_encode({
            "datacenter_tree": dc_tree
        }))
