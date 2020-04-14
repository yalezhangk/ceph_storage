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

object_store_init_schema = {
    "type": "object",
    "properties": {
        "object_store_init": {
            "type": "integer",
        },
    },
    "additionalProperties": False,
    "required": ["object_store_init"]
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

        exact_filters = ["node_id"]
        fuzzy_filters = ["display_name"]
        filters = self.get_support_filters(exact_filters, fuzzy_filters)

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
    def get(self, radosgw_id):
        """ Radosgw Detail

        ---
        tags:
        -  Radosgw
        summary: Detail of the  Radosgw
        description: Return detail infomation of  Radosgw by id
        operationId: Radosgw.api.Radosgw_detail
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
          description:  Radosgw ID
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
        radosgw = yield client.radosgw_get(
            ctxt, radosgw_id)
        self.write(objects.json_encode({"radosgw": radosgw}))

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


@URLRegistry.register(r"/radosgws/init/")
class RadosgwInitHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """Get object store init infos

        ---
        tags:
        - radosgw
        summary: Get object store init infos
        description: Get object store init infos
        operationId: radosgws.api.getObjectStoreInit
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
        status = yield client.object_store_init_get(ctxt)
        self.write(objects.json_encode({
            "object_store_status": status
        }))

    @gen.coroutine
    def post(self):
        """Initailize object store.

        ---
        tags:
        - radosgw
        summary: Set object store init infos
        description: Set object store init infos
        operationId: radosgws.api.setObjectStoreInit
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
          name: object_store_init
          description: Created osd object
          required: false
          schema:
            type: object
            properties:
                object_store_init:
                  type: integer
                  format: int32
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=object_store_init_schema,
                 format_checker=draft7_format_checker)
        index_pool_id = data.get("object_store_init")
        client = self.get_admin_client(ctxt)
        status = yield client.object_store_init(ctxt, index_pool_id)
        self.write(objects.json_encode({
            "object_store_status": status
        }))


@URLRegistry.register(r"/radosgws/([0-9]*)/metrics/")
class RadosgwMetricsHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, rgw_id):
        """
        ---
        tags:
        - radosgw
        summary: radosgw's Metrics
        description: return the Metrics of radosgw by id
        operationId: radosgw.api.getMetrics
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
          description: radosgw's id
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
        data = yield client.radosgw_metrics_get(
            ctxt, rgw_id=rgw_id)
        self.write(objects.json_encode({
            "radosgw_metrics": data
        }))


@URLRegistry.register(r"/radosgws/([0-9]*)/history_metrics/")
class RadosgwMetricsHistoryHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, rgw_id):
        """
        ---
        tags:
        - radosgw
        summary: radosgw's History Metrics
        description: return the History Metrics of radosgw by id
        operationId: radosgw.api.getHistoryMetrics
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
          description: radosgw's id
          schema:
            type: integer
            format: int32
          required: true
        - in: request
          name: start
          description: the start of the history, it must be a time stamp.
                       eg.1573600118.935
          schema:
            type: integer
            format: int32
          required: true
        - in: request
          name: end
          description: the end of the history, it must be a time stamp.
                       eg.1573600118.936
          schema:
            type: integer
            format: int32
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        his_args = self.get_metrics_history_args()
        client = self.get_admin_client(ctxt)
        data = yield client.radosgw_metrics_history_get(
            ctxt, rgw_id=rgw_id, start=his_args['start'],
            end=his_args['end'])
        self.write(objects.json_encode({
            "radosgw_history_metrics": data
        }))
