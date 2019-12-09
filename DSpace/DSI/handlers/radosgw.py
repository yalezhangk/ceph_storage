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


create_radosgw_schema = {
    "type": "object",
    "properties": {
        "radosgw": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "minLength": 5,
                    "maxLength": 32
                },
                "description": {"type": ["string", "null"]},
                "node_id": {"type": "integer"},
                "ip_address": {"type": "string", "format": "ipv4"},
                "port": {
                    "type": "integer",
                    "minimum": CONF.rgw_min_port,
                    "maximum": CONF.rgw_max_port
                },
            },
            "required": ["name", "node_id", "ip_address", "port"],
        },
    },
    "additionalProperties": False,
    "required": ["radosgw"]
}


@URLRegistry.register(r"/radosgws/")
class RadosgwListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """Get radosgws

        ---
        tags:
        - radosgw
        summary: return a list of rados gateways
        description: return a list of rados gateways
        operationId: radosgws.api.listRadosgw
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
          name: node
          description: Node ID
          type: string
          required: false
        responses:
        "200":
          description: successful operation
        """
        context = self.get_context()
        page_args = self.get_paginated_args()

        node_id = self.get_query_argument('node', default=None)
        filters = {}
        if node_id:
            filters.update({
                'node_id': node_id
            })

        client = self.get_admin_client(context)
        radosgws = yield client.radosgw_get_all(
            context, filters=filters, **page_args)
        radosgws_count = yield client.radosgw_get_count(
            context, filters=filters)

        self.write(objects.json_encode({
            "rgws": radosgws,
            "total": radosgws_count
        }))

    @gen.coroutine
    def post(self):
        """Create radosgw

        radosgw:
        {
            "name": string,
            "description": string,
            "node_id": number,
            "ip_address": string,
            "port": number,
        }
        ---
        tags:
        - radosgw
        summary: Create radosgw
        description: Create radosgw or radosgws.
        operationId: radosgws.api.createRadosgw
        produces:
        - application/json
        parameters:
        - in: body
          name: radosgw
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
                      description: radosgw's name
                    description:
                      type: string
                      description: the description of radosgw
                    node_id:
                      type: integer
                      format: int32
                    ip_address:
                      type: string
                      description: the ip address of radosgw
                    port:
                      type: integer
                      format: int32
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=create_radosgw_schema,
                 format_checker=draft7_format_checker)
        data = data.get('radosgw')
        logger.debug("Create radosgw data: %s", data)
        client = self.get_admin_client(ctxt)
        radosgw = yield client.radosgw_create(ctxt, data)
        self.write(objects.json_encode({
            "radosgw": radosgw
        }))


@URLRegistry.register(r"/radosgws/([0-9]*)/")
class RadosgwHandler(ClusterAPIHandler):
    @gen.coroutine
    def delete(self, rgw_id):
        """Delete radosgw

        ---
        tags:
        - radosgw
        summary: Delete the radosgw by id
        description: delete radosgw by id
        operationId: radosgws.api.deleteRadosgw
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
          description: Radosgw's id
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
        radosgw = yield client.radosgw_delete(ctxt, rgw_id)
        self.write(objects.json_encode({
            "radosgw": radosgw
        }))


@URLRegistry.register(r"/radosgws/infos/")
class RadosgwInfoHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """Get radosgw infos

        ---
        tags:
        - radosgw
        summary: Get Radosgw infos
        description: Get Radosgw infos
        operationId: radosgws.api.getRadosgwInfos
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
        self.write(objects.json_encode({
            "radosgw_infos": {
                "rgw_min_port": CONF.rgw_min_port,
                "rgw_max_port": CONF.rgw_max_port
            }
        }))
