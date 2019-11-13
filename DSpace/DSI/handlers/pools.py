import json
import logging

from tornado import gen
from tornado.escape import json_decode

from DSpace import objects
from DSpace.DSI.handlers.base import ClusterAPIHandler
from DSpace.exception import InvalidInput
from DSpace.i18n import _

logger = logging.getLogger(__name__)


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

        tab = self.get_query_argument('tab', default="default")
        if tab not in ["default", "io"]:
            raise InvalidInput(_("this tab is not supported"))

        expected_attrs = ['crush_rule', 'osds', 'volumes']
        pools = yield client.pool_get_all(ctxt, expected_attrs=expected_attrs,
                                          tab=tab, **page_args)
        pool_count = yield client.pool_get_count(ctxt)
        self.write(objects.json_encode({
            "pools": pools,
            "total": pool_count
        }))

    @gen.coroutine
    def post(self):
        """创建存储池
        pool:
        {
            "name": string,
            "type": string[replicated,erasure],
            "speed_type": string[hdd,ssd],
            "role": string[data,metadata],
            "data_chunk_num": number,
            "coding_chunk_num": number,
            "replicated_size": number,
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
                  replicated_size:
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
        data = json_decode(self.request.body).get('pool')
        logger.debug("create pool data: {}".format(data))
        client = self.get_admin_client(ctxt)
        pool = yield client.pool_create(ctxt, data)
        self.write(objects.json_encode({
            "pool": pool
        }))


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
        """编辑存储池

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
        data = json_decode(self.request.body).get('pool')
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
        """删除存储池
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
        client = self.get_admin_client(ctxt)
        pool = yield client.pool_delete(
            ctxt, pool_id)
        self.write(objects.json_encode({
            "pool": pool
        }))


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


class PoolIncreaseDiskHandler(ClusterAPIHandler):
    @gen.coroutine
    def post(self, pool_id):
        """添加磁盘

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
        data = json_decode(self.request.body).get('pool')
        logger.debug("increase disk: {}".format(data))
        client = self.get_admin_client(ctxt)
        pool = yield client.pool_increase_disk(
            ctxt, pool_id, data)
        self.write(objects.json_encode({
            "pool": pool
        }))


class PoolDecreaseDiskHandler(ClusterAPIHandler):
    @gen.coroutine
    def post(self, pool_id):
        """移除磁盘

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
        data = json_decode(self.request.body).get('pool')
        logger.debug("decrease disk: {}".format(data))
        client = self.get_admin_client(ctxt)
        pool = yield client.pool_decrease_disk(
            ctxt, pool_id, data)
        self.write(objects.json_encode({
            "pool": pool
        }))


class PoolPolicyHandler(ClusterAPIHandler):
    @gen.coroutine
    def post(self, pool_id):
        """修改存储池安全策略

        {"pool":{"rep_size":3,"fault_domain":"rack"}}

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
                  rep_size:
                    type: integer
                    format: int32
                  fault_domain:
                    type: string
                    description: fault domain's Level
        responses:
        "200":
          description: successful operation
        """
        ctxt = self.get_context()
        data = json_decode(self.request.body).get('pool')
        logger.debug("update security policy: {}".format(data))
        client = self.get_admin_client(ctxt)
        pool = yield client.pool_update_policy(
            ctxt, pool_id, data)
        self.write(objects.json_encode({
            "pool": pool
        }))


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
