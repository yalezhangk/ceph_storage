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
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        page_args = self.get_paginated_args()
        expected_attrs = ['crush_rule', 'osds', 'volumes']
        pools = yield client.pool_get_all(ctxt, expected_attrs=expected_attrs,
                                          **page_args)
        pools_all = yield client.pool_get_all(ctxt,
                                              expected_attrs=expected_attrs)
        self.write(objects.json_encode({
            "pools": pools,
            "total": len(pools_all)
        }))

    @gen.coroutine
    def post(self):
        """创建存储池
        pool:
        {
            "name": string,
            "type": string[replicated,erasure],
            "speed_type": string[HDD,SSD],
            "role": string[data,metadata],
            "data_chunk_num": number,
            "coding_chunk_num": number,
            "replicated_size": number,
            "failure_domain_type": string[host,rack,datacenter],
            'osds', [1,3]
        }
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
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        osds = yield client.pool_osds_get(ctxt, pool_id)
        self.write(objects.json_encode({"osds": osds}))


class PoolCapacityHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        """TODO 存储池容量"""
        self.write(objects.json_encode({}))


class PoolIncreaseDiskHandler(ClusterAPIHandler):
    @gen.coroutine
    def post(self, pool_id):
        """添加磁盘

        {"pool":{"osds":[1,2,3]}}
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
        ctxt = self.get_context()
        client = self.get_admin_client(ctxt)
        data = yield client.pool_metrics_get(ctxt, pool_id=pool_id)
        self.write(json.dumps({
            "pool_metrics": data
        }))


class PoolMetricsHistoryHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, pool_id):
        ctxt = self.get_context()
        his_args = self.get_metrics_history_args()
        client = self.get_admin_client(ctxt)
        data = yield client.pool_metrics_history_get(
            ctxt, pool_id=pool_id, start=his_args['start'],
            end=his_args['end'])
        self.write(json.dumps({
            "pool_metrics_history": data
        }))
