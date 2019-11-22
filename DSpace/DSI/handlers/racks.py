#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from jsonschema import draft7_format_checker
from jsonschema import validate
from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


create_rack_schame = {
    "type": "object",
    "properties": {
        "datacenter_id": {
            "type": "integer",
            "minimum": 1
        },
    },
    "required": ["datacenter_id"],
}

update_rack_schame = {
    "type": "object",
    "properties": {
        "rack": {
            "type": "object",
            "properties": {
                "datacenter_id": {"type": "integer", "minimum": 1},
                "name": {
                    "type": "string",
                    "minLength": 5,
                    "maxLength": 32
                }
            },
            "anyOf": [{"required": ["datacenter_id"]}, {"required": ["name"]}]
        },
    },
    "required": ["rack"]
}


@URLRegistry.register(r"/racks/")
class RackListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """获取机架列表信息
        ---
        tags:
        - rack
        summary: rack List
        description: Return a list of racks
        operationId: racks.api.listRack
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
        racks = yield client.rack_get_all(ctxt)
        self.write(objects.json_encode({
            "racks": racks
        }))

    @gen.coroutine
    def post(self):
        """创建机架

        {"datacenter_id": 1}

        ---
        tags:
        - rack
        summary: Create rack
        description: create rack.
        operationId: racks.api.createRack
        produces:
        - application/json
        parameters:
        - in: header
          name: X-Cluster-Id
          description: Cluster ID
          schema:
            type: string
          required: true
        - in: body
          name: rack
          description: create rack object
          required: true
          schema:
            type: object
            properties:
              datacenter:
                type: integer
                format: int32
                description: ID of the datacenter to which the rack belongs
        """
        data = json_decode(self.request.body)
        validate(data, schema=create_rack_schame,
                 format_checker=draft7_format_checker)
        datacenter_id = data.get('datacenter_id')
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        rack = yield client.rack_create(ctxt, datacenter_id)
        logger.debug(
            "rack: id: {}, name: {}, datacenter_id, cluster_id: {}".format(
                rack.id, rack.name, rack.datacenter_id, rack.cluster_id))
        self.write(objects.json_encode({
            "rack": rack
        }))


@URLRegistry.register(r"/racks/([0-9]*)/")
class RackHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, rack_id):
        """获取机架信息
        ---
        tags:
        - rack
        summary: Detail of the rack
        description: Return detail infomation of rack by id
        operationId: racks.api.rackDetail
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
          description: rack ID
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
        rack = yield client.rack_get(ctxt, rack_id)
        self.write(objects.json_encode({
            "rack": rack
        }))

    @gen.coroutine
    def put(self, rack_id):
        """修改机架信息：机架名称或所属的数据中心

        {"rack": {"name":"rack-name"}}
        {"rack": {"datacenter_id": 1}}

        ---
        tags:
        - rack
        summary: Update rack
        description: update rack.
        operationId: racks.api.updateRack
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
          description: rack ID
          schema:
            type: integer
            format: int32
          required: true
        - in: body
          name: rack(update name)
          description: updated rack object's name
          required: false
          schema:
            type: object
            properties:
              rack:
                type: object
                properties:
                  name:
                    type: string
                    description: rack's name
        - in: body
          name: rack(update datacenter)
          description: move rack to another datacenter
          required: false
          schema:
            type: object
            properties:
              rack:
                type: object
                properties:
                  datacenter:
                    type: integer
                    format: int32
                    description: the datacenter's id
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        body = json_decode(self.request.body)
        validate(body, schema=update_rack_schame,
                 format_checker=draft7_format_checker)
        data = body.get('rack')
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
        ---
        tags:
        - rack
        summary: Delete the rack by id
        description: delete rack by id
        operationId: racks.api.deleteRack
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
          description: rack's id
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
        rack = yield client.rack_delete(ctxt, rack_id)
        self.write(objects.json_encode({
            "rack": rack
        }))


class RackHostsHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, rack_id):
        """
        ---
        tags:
        - rack
        summary: Get the virtual host
        description: Get the virtual host for the rack
        operationId: racks.api.getHost
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
          description: rack's id
          schema:
            type: integer
            format: int32
          required: true
        responses:
        "200":
          description: successful operation
        """
        # TODO 获取机架下的虚拟host
        pass
