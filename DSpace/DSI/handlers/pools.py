import json
import logging

from jsonschema import draft7_format_checker
from jsonschema import validate
from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers import URLRegistry
from DSpace.DSI.handlers.base import ClusterAPIHandler
from DSpace.exception import InvalidInput
from DSpace.i18n import _

logger = logging.getLogger(__name__)


create_pool_schema = {
    "type": "object",
    "properties": {
        "pool": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "minLength": 5,
                    "maxLength": 32
                },
                "type": {
                    "type": "string",
                    "enum": ["replicated", "erasure"]
                },
                "speed_type": {"type": "string"},
                "role": {"type": "string"},
                "failure_domain_type": {
                    "type": "string",
                    "enum": ["osd", "host", "rack", "datacenter"]
                },
                "osds": {
                    "type": "array",
                    "items": {"type": "integer", "minimum": 1},
                    "minItems": 1,
                    "uniqueItems": True
                }
            },
            "required": ["name", "type", "role",
                         "failure_domain_type", "osds"],
            "allOf": [
                {
                    "if": {
                        "properties": {"type": {"const": "replicated"}}
                    },
                    "then": {
                        "properties": {
                            "replicate_size": {
                                "type": "integer",
                                "minimum": 1,
                                "maximum": 6
                            },
                        },
                        "required": ["replicate_size"]
                    }
                }, {
                    "if": {
                        "properties": {"type": {"const": "erasure"}}
                    },
                    "then": {
                        "properties": {
                            "data_chunk_num": {
                                "type": "integer",
                                "minimum": 1
                            },
                            "coding_chunk_num": {
                                "type": "integer",
                                "minimum": 1
                            },
                        },
                        "required": ["data_chunk_num", "coding_chunk_num"]
                    }
                },
            ],
        },
    },
    "additionalProperties": False,
    "required": ["pool"]
}

update_pool_schema = {
    "type": "object",
    "properties": {
        "pool": {
            "type": "object",
            "properties": {"name": {
                "type": "string",
                "minLength": 5,
                "maxLength": 32
            }}, "required": ["name"],
            "additionalProperties": False
        },
    },
    "required": ["pool"],
    "additionalProperties": False
}

update_pool_disk_schema = {
    "type": "object",
    "properties": {
        "pool": {
            "type": "object",
            "properties": {
                "osds": {
                    "type": "array",
                    "items": {"type": "integer", "minimum": 1},
                    "minItems": 1,
                    "uniqueItems": True
                }
            }, "required": ["osds"],
        },
    },
    "required": ["pool"],
    "additionalProperties": False
}

update_pool_security_policy_schema = {
    "type": "object",
    "properties": {
        "pool": {
            "type": "object",
            "properties": {
                "replicate_size": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 6
                },
                "failure_domain_type": {
                    "type": "string",
                    "enum": ["osd", "host", "rack", "datacenter"]
                },
            }, "required": ["replicate_size", "failure_domain_type"],
        },
    },
    "required": ["pool"],
    "additionalProperties": False
}


@URLRegistry.register(r"/pools/block/")
class BlockPoolListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        filters = {"role": "data"}
        pools = yield client.pool_get_all(
                ctxt, expected_attrs=None,
                filters=filters)
        self.write(objects.json_encode({
            "pools": pools}
            ))


@URLRegistry.register(r"/pools/")
class PoolListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - pool
        summary: Pool List
        description: Return a list of pools
        operationId: pools.api.listPool
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
        - in: request
          name: tab
          description: Different tab page, it can be default or io
          schema:
            type: string
          required: false
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        page_args = self.get_paginated_args()

        exact_filters = ['role', 'type', 'status', 'speed_type',
                         'failure_domain_type']
        fuzzy_filters = ['display_name']
        filters = self.get_support_filters(exact_filters, fuzzy_filters)

        tab = self.get_query_argument('tab', default="default")
        if tab not in ["default", "io"]:
            raise InvalidInput(_("this tab is not supported"))

        expected_attrs = ['crush_rule', 'osds', 'volumes']
        pools = yield client.pool_get_all(
            ctxt, expected_attrs=expected_attrs, tab=tab,
            filters=filters, **page_args)
        pool_count = yield client.pool_get_count(ctxt, filters=filters)
        self.write(objects.json_encode({
            "pools": pools,
            "total": pool_count
        }))

    @gen.coroutine
    def post(self):
        """???????????????
        pool:
        {
            "name": string,
            "type": string[replicated,erasure],
            "speed_type": string[hdd,ssd],
            "role": string[data,metadata],
            "data_chunk_num": number,
            "coding_chunk_num": number,
            "replicate_size": number,
            "failure_domain_type": string[host,rack,datacenter],
            'osds', [1,3]
        }
        ---
        tags:
        - pool
        summary: Create pool
        description: Create pool.
        operationId: pools.api.createPool
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
          name: pool
          description: Created pool object
          required: true
          schema:
            type: object
            properties:
              pool:
                type: object
                description: pool object
                properties:
                  name:
                    type: string
                    description: pool's name
                  type:
                    type: string
                    description: pool's type, it can be replicated/erasure
                  speed_type:
                    type: string
                    description: pool's speed type, it can be hdd/ssd
                  role:
                    type: string
                    description: pool's role, it can be data/metadata
                  data_chunk_num:
                    type: integer
                    format: int32
                  coding_chunk_num:
                    type: integer
                    format: int32
                  replicate_size:
                    type: integer
                    format: int32
                  failure_domain_type:
                    type: string
                    description: pool's failure_domain_type,
                                 it can be host/rack/datacenter
                  osds:
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
        validate(data, schema=create_pool_schema,
                 format_checker=draft7_format_checker)
        data = data.get('pool')
        logger.debug("create pool data: %s", data)
        client = self.get_admin_client(ctxt)
        pool = yield client.pool_create(ctxt, data)
        self.write(objects.json_encode({
            "pool": pool
        }))


@URLRegistry.register(r"/pools/([0-9]*)/")
class PoolHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, pool_id):
        """
        ---
        tags:
        - pool
        summary: Detail of the pool
        description: Return detail infomation of pool by id
        operationId: pools.api.poolDetail
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
          description: Pool ID
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
        expected_attrs = ['crush_rule', 'osds', 'volumes']
        pool = yield client.pool_get(
            ctxt, pool_id, expected_attrs=expected_attrs)
        self.write(objects.json_encode({"pool": pool}))

    @gen.coroutine
    def put(self, pool_id):
        """???????????????

        {"pool": {"name":"pool-name"}}

        ---
        tags:
        - pool
        summary: Update pool
        description: update pool.
        operationId: pools.api.updatePool
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
          description: Pool ID
          schema:
            type: integer
            format: int32
          required: true
        - in: body
          name: pool
          description: updated pool object
          required: true
          schema:
            type: object
            properties:
              pool:
                type: object
                properties:
                  name:
                    type: string
                    description: pool's name
        responses:
        "200":
          description: successful operation
        """
        data = json_decode(self.request.body)
        validate(data, schema=update_pool_schema,
                 format_checker=draft7_format_checker)
        data = data.get('pool')
        pool_name = data.get('name')
        if not pool_name:
            raise InvalidInput(reason=_("pool: name is none"))
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        pool = yield client.pool_update_display_name(
            ctxt, pool_id, pool_name)
        self.write(objects.json_encode({
            "pool": pool
        }))

    @gen.coroutine
    def delete(self, pool_id):
        """???????????????
        ---
        tags:
        - pool
        summary: Delete the pool by id
        description: delete pool by id
        operationId: pools.api.deletePool
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
          description: Pool's id
          schema:
            type: integer
            format: int32
          required: true
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        logger.info("trying to delete pool: %s", pool_id)
        client = self.get_admin_client(ctxt)
        pool = yield client.pool_delete(
            ctxt, pool_id)
        logger.info("delete pool: %s success", pool_id)
        self.write(objects.json_encode({
            "pool": pool
        }))


@URLRegistry.register(r"/pools/([0-9]*)/osds/")
class PoolOsdsHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, pool_id):
        """
        ---
        tags:
        - pool
        summary: Osds of the pool
        description: Return osd infomation of pool by id
        operationId: pools.api.poolOsds
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
          description: Pool ID
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
        expected_attrs = ['disk', 'node']
        osds = yield client.pool_osds_get(
            ctxt, pool_id, expected_attrs=expected_attrs)
        self.write(objects.json_encode({"osds": osds}))


@URLRegistry.register(r"/pools/([0-9]*)/capacity/")
class PoolCapacityHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, pool_id):
        """
        ---
        tags:
        - pool
        summary: Pool's Capacity
        description: return the Capacity of pool by id
        operationId: pools.api.getCapacity
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
          description: Pool's id
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
        data = yield client.pool_capacity_get(ctxt, pool_id)
        self.write(objects.json_encode({
            "pool_capacity": data
        }))


@URLRegistry.register(r"/pools/([0-9]*)/increase_disk/")
class PoolIncreaseDiskHandler(ClusterAPIHandler):
    @gen.coroutine
    def post(self, pool_id):
        """????????????

        {"pool":{"osds":[1,2,3]}}

        ---
        tags:
        - pool
        summary: add osd to pool
        description: add osd to pool by id.
        operationId: pools.api.addOSD
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
          description: Pool ID
          schema:
            type: integer
            format: int32
          required: true
        - in: body
          name: pool
          description: updated pool object
          required: true
          schema:
            type: object
            properties:
              pool:
                type: object
                properties:
                  osds:
                    type: array
                    items:
                      type: integer
                      format: int32
                      description: osd's ID
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=update_pool_disk_schema,
                 format_checker=draft7_format_checker)
        data = data.get('pool')
        logger.debug("increase disk: {}".format(data))
        client = self.get_admin_client(ctxt)
        pool = yield client.pool_increase_disk(
            ctxt, pool_id, data)
        self.write(objects.json_encode({
            "pool": pool
        }))


@URLRegistry.register(r"/pools/([0-9]*)/decrease_disk/")
class PoolDecreaseDiskHandler(ClusterAPIHandler):
    @gen.coroutine
    def post(self, pool_id):
        """????????????

        {"pool":{"osds":[1,2,3]}}

        ---
        tags:
        - pool
        summary: remove pool's osd
        description: remove the pool's osd by id.
        operationId: pools.api.removeOSD
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
          description: Pool ID
          schema:
            type: integer
            format: int32
          required: true
        - in: body
          name: pool
          description: updated pool object
          required: true
          schema:
            type: object
            properties:
              pool:
                type: object
                properties:
                  osds:
                    type: array
                    items:
                      type: integer
                      format: int32
                      description: osd's ID
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=update_pool_disk_schema,
                 format_checker=draft7_format_checker)
        data = data.get('pool')
        logger.debug("decrease disk: {}".format(data))
        client = self.get_admin_client(ctxt)
        pool = yield client.pool_decrease_disk(
            ctxt, pool_id, data)
        self.write(objects.json_encode({
            "pool": pool
        }))


@URLRegistry.register(r"/pools/([0-9]*)/update_security_policy/")
class PoolPolicyHandler(ClusterAPIHandler):
    @gen.coroutine
    def post(self, pool_id):
        """???????????????????????????

        {"pool":{"replicate_size":3,"failure_domain_type":"rack"}}
        {"pool":{"replicate_size":3,"failure_domain_type":"datacenter"}}

        ---
        tags:
        - pool
        summary: Modify pool's security policy
        description: Modify Apple's security policy.
        operationId: pools.api.addOSD
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
          description: Pool ID
          schema:
            type: integer
            format: int32
          required: true
        - in: body
          name: pool
          description: updated pool object
          required: true
          schema:
            type: object
            properties:
              pool:
                type: object
                properties:
                  replicate_size:
                    type: integer
                    format: int32
                  failure_domain_type:
                    type: string
                    description: fault domain's Level
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        validate(data, schema=update_pool_security_policy_schema,
                 format_checker=draft7_format_checker)
        data = data.get('pool')
        logger.debug("update security policy: {}".format(data))
        client = self.get_admin_client(ctxt)
        pool = yield client.pool_update_policy(
            ctxt, pool_id, data)
        self.write(objects.json_encode({
            "pool": pool
        }))


@URLRegistry.register(r"/pools/([0-9]*)/metrics/")
class PoolMetricsHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, pool_id):
        """
        ---
        tags:
        - pool
        summary: Pool's Metrics
        description: return the Metrics of pool by id
        operationId: pools.api.getMetrics
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
          description: Pool's id
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
        data = yield client.pool_metrics_get(ctxt, pool_id=pool_id)
        self.write(json.dumps({
            "pool_metrics": data
        }))


@URLRegistry.register(r"/pools/([0-9]*)/history_metrics/")
class PoolMetricsHistoryHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, pool_id):
        """
        ---
        tags:
        - pool
        summary: Pool's History Metrics
        description: return the History Metrics of pool by id
        operationId: pools.api.getHistoryMetrics
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
          description: Pool's id
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
        data = yield client.pool_metrics_history_get(
            ctxt, pool_id=pool_id, start=his_args['start'],
            end=his_args['end'])
        self.write(json.dumps({
            "pool_metrics_history": data
        }))


@URLRegistry.register(r"/pools/([0-9]*)/osd_tree/")
class PoolOsdTreeHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, pool_id):
        """
        ---
        tags:
        - pool
        summary: Pool's Osd Tree
        description: return the osd tree of pool by id
        operationId: pools.api.osdTree
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
          description: Pool's id
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
        logger.info("trying to get osd tree for pool %s", pool_id)
        data = yield client.pool_osd_tree(
            ctxt, pool_id=pool_id)
        self.write(objects.json_encode({
            "pool_osd_tree": data
        }))
        logger.info("get osd tree success")


@URLRegistry.register(r"/pools/undo/")
class PoolUndoHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """
        ---
        tags:
        - pool
        summary: Pool undo info
        description: pool undo info
        operationId: pools.api.undoInfo
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
        logger.info("trying pool undo")
        data = yield client.pool_get_undo(ctxt)
        self.write(objects.json_encode({
            "pool_undo": data
        }))
        logger.info("get undo op accept")

    @gen.coroutine
    def post(self):
        """
        ---
        tags:
        - pool
        summary: Pool undo
        description: pool undo
        operationId: pools.api.undo
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
        logger.info("trying pool undo")
        pool = yield client.pool_undo(ctxt)
        self.write(objects.json_encode({
            "pool": pool
        }))
        logger.info("get undo op accept")
