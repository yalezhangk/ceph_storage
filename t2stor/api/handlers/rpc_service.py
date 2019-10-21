#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

import six
from tornado import gen
from tornado.escape import json_decode
from tornado.escape import json_encode

from t2stor import objects
from t2stor.api.handlers.base import ClusterAPIHandler

logger = logging.getLogger(__name__)


class RpcServiceListHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self):
        ctxt = self.get_context()
        rpc_service = objects.RPCServiceList.get_all(ctxt)
        self.write(json_encode({
            "RPCServices": [
                {
                    "id": r.id,
                    "hostname": r.hostname,
                    "endpoint": json_decode(r.endpoint),
                    "service_name": r.service_name,
                    "cluster_id": r.cluster_id,
                } for r in rpc_service
            ]
        }))

    @gen.coroutine
    def post(self):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        data = data.get("rpc_service")
        r = objects.RPCService(
            ctxt, cluster_id=ctxt.cluster_id, hostname=data.get('hostname'),
            service_name=data.get('service_name'),
            endpoint=json_encode(data.get('endpoint'))
        )
        r.create()
        self.write(json_encode({
            "rpc_service": {
                "id": r.id,
                "hostname": r.hostname,
                "endpoint": json_decode(r.endpoint),
                "service_name": r.service_name,
                "cluster_id": r.cluster_id,
            }
        }))


class RpcServiceHandler(ClusterAPIHandler):
    @gen.coroutine
    def get(self, rpc_service_id):
        ctxt = self.get_context()
        r = objects.RPCServiceList.get_by_id(ctxt, rpc_service_id)
        self.write(json_encode({
            "rpc_service": {
                "id": r.id,
                "hostname": r.hostname,
                "endpoint": json_decode(r.endpoint),
                "service_name": r.service_name,
                "cluster_id": r.cluster_id,
            }
        }))

    @gen.coroutine
    def post(self, rpc_service_id):
        ctxt = self.get_context()
        data = json_decode(self.request.body)
        rpc_service_data = data.get("rpc_service")
        r = objects.RPCServiceList.get_by_id(ctxt, rpc_service_id)
        for k, v in six.iteritems(rpc_service_data):
            if k != "endpoint":
                setattr(r, k, v)
            else:
                setattr(r, k, json_encode(v))

        r.save()
        self.write(json_encode({
            "rpc_service": {
                "id": r.id,
                "hostname": r.hostname,
                "endpoint": json_decode(r.endpoint),
                "service_name": r.service_name,
                "cluster_id": r.cluster_id,
            }
        }))

    @gen.coroutine
    def delete(self, rpc_service_id):
        ctxt = self.get_context()
        r = objects.RPCServiceList.get_by_id(ctxt, rpc_service_id)
        r.delete()
        self.write(json_encode({
            "rpc_service": {
                "id": r.id,
                "hostname": r.hostname,
                "endpoint": json_decode(r.endpoint),
                "service_name": r.service_name,
                "cluster_id": r.cluster_id,
            }
        }))
