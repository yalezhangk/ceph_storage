#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

import six
from jsonschema import draft7_format_checker
from jsonschema import validate
from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


create_rpc_service_schema = {
    "type": "object",
    "properties": {
        "rpc_service": {
            "type": "object",
            "properties": {
                "hostname": {"type": "string", "format": "hostname"},
                "cluster_id": {
                    "type": "string",
                    "minLength": 36,
                    "maxLength": 36
                },
                "service_name": {"type": "string"},
                "endpoint": {
                    "type": "object",
                    "properties": {
                        "ip": {"type": "string", "format": "ipv4"},
                        "port": {
                            "type": "integer",
                            "minimum": 0,
                            "maximum": 65535
                        }
                    },
                    "required": ["ip", "port"]
                }
            },
            "additionalProperties": False,
            "required": ["service_name", "endpoint"]
        },
    },
    "required": ["rpc_service"],
    "additionalProperties": False
}


@URLRegistry.register(r"/rpc_services/")
class RpcServiceListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - rpc_service
        summary: rpc_service List
        description: Return a list of rpc_services
        operationId: rpc_services.api.listRpc_service
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
        rpc_service = objects.RPCServiceList.get_all(ctxt)
        self.write(objects.json_encode({
            "rpc_services": rpc_service
        }))

    @gen.coroutine
    def post(self):
        """
        ---
        tags:
        - rpc_service
        summary: Create rpc_service
        description: Create rpc_service.
        operationId: rpc_services.api.createRpc_service
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
          name: rpc_service
          description: Created rpc_service object
          required: true
          schema:
            type: object
            properties:
              rpc_service:
                type: object
                description: rpc_service object
                properties:
                  hostname:
                    type: string
                  cluster_id:
                    type: string
                  service_name:
                    type: string
                  endpoint:
                    type: object
                    properties:
                      ip:
                        type: string
                      port:
                        type: string
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=create_rpc_service_schema,
                 format_checker=draft7_format_checker)
        data = data.get("rpc_service")
        r = objects.RPCService(
            ctxt, cluster_id=ctxt.cluster_id, hostname=data.get('hostname'),
            service_name=data.get('service_name'),
            endpoint=data.get('endpoint')
        )
        r.create()
        self.write(objects.json_encode({
            "rpc_service": r
        }))


class RpcServiceHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, rpc_service_id):
        ctxt = self.get_context()
        r = objects.RPCServiceList.get_by_id(ctxt, rpc_service_id)
        self.write(objects.json_encode({
            "rpc_service": r
        }))

    @gen.coroutine
    def post(self, rpc_service_id):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        rpc_service_data = data.get("rpc_service")
        r = objects.RPCServiceList.get_by_id(ctxt, rpc_service_id)
        for k, v in six.iteritems(rpc_service_data):
            setattr(r, k, v)

        r.save()
        self.write(objects.json_encode({
            "rpc_service": r
        }))

    @gen.coroutine
    def delete(self, rpc_service_id):
        ctxt = self.get_context()
        r = objects.RPCServiceList.get_by_id(ctxt, rpc_service_id)
        r.delete()
        self.write(objects.json_encode({
            "rpc_service": r
        }))
