import logging

from jsonschema import draft7_format_checker
from jsonschema import validate
from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.common.config import CONF
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


create_router_schema = {
    "type": "object",
    "properties": {
        "radosgw_router": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "minLength": 5,
                    "maxLength": 32
                },
                "description": {"type": ["string", "null"]},
                "virtual_ip": {"type": ["string", "null"], "format": "ipv4"},
                "virtual_router_id": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 255
                },
                "port": {
                    "type": "integer",
                    "minimum": CONF.rgw_min_port,
                    "maximum": CONF.rgw_max_port
                },
                "https_port": {
                    "type": "integer",
                    "minimum": CONF.rgw_min_port,
                    "maximum": CONF.rgw_max_port
                },
                "nodes": {
                    "type": "array",
                    "items": {"type": "object", "minimum": 1},
                    "minItems": 1,
                    "uniqueItems": True
                },
                "radosgws": {
                    "type": "array",
                    "items": {"type": "integer", "minimum": 1},
                    "minItems": 1,
                    "uniqueItems": True
                }
            },
            "required": ["name", "virtual_ip", "virtual_router_id", "port",
                         "https_port", "nodes", "radosgws"],
        },
    },
    "additionalProperties": False,
    "required": ["radosgw_router"]
}

update_router_schema = {
    "type": "object",
    "properties": {
        "radosgw_router": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["add", "remove"]
                },
                "radosgws": {
                    "type": "array",
                    "items": {"type": "integer", "minimum": 1},
                    "minItems": 1,
                    "uniqueItems": True
                }
            },
            "required": ["action", "radosgws"],
        },
    },
    "additionalProperties": False,
    "required": ["radosgw_router"]
}


@URLRegistry.register(r"/radosgw_routers/")
class RadosgwRouterListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """Get radosgw routers

        ---
        tags:
        - radosgw_router
        summary: return a list of rados gateways
        description: return a list of rados gateways
        operationId: radosgw_router.api.listRadosgwRouter
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
        context = self.get_context()
        page_args = self.get_paginated_args()
        exact_filters = []
        fuzzy_filters = ["name"]
        filters = self.get_support_filters(exact_filters, fuzzy_filters)
        client = self.get_admin_client(context)
        rgw_routers = yield client.rgw_router_get_all(
            context, filters=filters, **page_args)
        rgw_routers_count = yield client.rgw_router_get_count(
            context, filters=filters)

        self.write(objects.json_encode({
            "rgw_routers": rgw_routers,
            "total": rgw_routers_count
        }))

    @gen.coroutine
    def post(self):
        """Create radosgw

        radosgw_router:
        {
          "name": string,
          "description": string,
          "virtual_ip": string,
          "virtual_router_id": number,
          "port": number,
          "nodes": [
            {
              "node_id": 7,
              "net_id": 7
            },
            {
              "node_id": 10,
              "net_id": 13
            }
          ],
          "radosgws": [25, 28]
        }

        ---
        tags:
        - radosgw_router
        summary: Create radosgw router
        description: Create radosgw router.
        operationId: radosgw_router.api.createRadosgwRouter
        produces:
        - application/json
        parameters:
        - in: body
          name: radosgw_router
          description: Created osd object
          required: false
          schema:
            type: object
            properties:
                radosgw:
                  type: object
                  properties:
                    name:
                      type: string
                      description: radosgw router's name
                    description:
                      type: string
                      description: the description of radosgw
                    virtual_router_id:
                      type: integer
                      format: int32
                      description: the router_id of keepalived
                    virtual_ip:
                      type: string
                      description: the router_id ip address of radosgw router
                    port:
                      type: integer
                      format: int32
                    nodes:
                      type: array
                      items:
                        type: object
                    radosgws:
                      type: array
                      items:
                        type: integer
                        format: int32
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=create_router_schema,
                 format_checker=draft7_format_checker)
        data = data.get('radosgw_router')
        client = self.get_admin_client(ctxt)
        rgw_router = yield client.rgw_router_create(ctxt, data)
        self.write(objects.json_encode({
            "rgw_router": rgw_router
        }))


@URLRegistry.register(r"/radosgw_routers/([0-9]*)/")
class RadosgwRouterHandler(ClusterAPIHandler):
    @gen.coroutine
    def delete(self, rgw_router_id):
        """Delete Radosgw router

        ---
        tags:
        - radosgw_router
        summary: Delete the router by id
        description: delete router by id
        operationId: radosgw_router.api.deleteRadosgwRouter
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
          description: router's id
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
        rgw_router = yield client.rgw_router_delete(ctxt, rgw_router_id)
        self.write(objects.json_encode({
            "rgw_routers": rgw_router
        }))

    @gen.coroutine
    def put(self, rgw_router_id):
        """Update Radosgw Router

        {
          "radosgw_router": {
            "action": string["remove", "add"],
            "radosgws": [16]
          }
        }

        ---
        tags:
        - radosgw_router
        summary: Update radosgw router
        description: update radosgw_router.
        operationId: radosgw_router.api.updateRadosgwRouter
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
          description: radosgw router ID
          schema:
            type: integer
            format: int32
          required: true
        - in: body
          name: radosgw_router
          description: updated radosgw router object
          required: true
          schema:
            type: object
            properties:
              radosgw_router:
                type: object
                properties:
                  action:
                    type: string
                    description: remove or add action
                  radosgws:
                    type: array
                    items:
                      type: integer
                      format: int32
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=update_router_schema,
                 format_checker=draft7_format_checker)
        data = data.get('radosgw_router')
        client = self.get_admin_client(ctxt)
        rgw_router = yield client.rgw_router_update(
            ctxt, rgw_router_id, data)
        self.write(objects.json_encode({
            "rgw_routers": rgw_router
        }))
