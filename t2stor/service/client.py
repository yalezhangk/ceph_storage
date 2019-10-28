from __future__ import print_function

import json
import logging

import grpc
from tornado.gen import Future
from tornado.ioloop import IOLoop

from t2stor import exception
from t2stor import objects
from t2stor.objects import base as objects_base
from t2stor.service import stor_pb2
from t2stor.service import stor_pb2_grpc
from t2stor.service.serializer import RequestContextSerializer

logger = logging.getLogger(__name__)


class BaseClientManager:
    """Client Manager

    cluster_id: Which cluster to connect.
    async_support: False, return value of rpc call.
                   True, return Future of rpc call.
    """
    cluster_id = None
    channel = None
    service_name = None
    endpoints = None
    client_cls = None

    def __init__(self, context, cluster_id=None, async_support=False):
        self.context = context
        self.async_support = async_support
        if cluster_id:
            self.cluster_id = cluster_id
        logger.debug("etcd server(%s) cluster_id(%s) service_name(%s)" % (
            "127.0.0.1", self.cluster_id, self.service_name
        ))

    def _get_endpoints_db(self):
        logger.debug("endpints search: cluster_id(%s), service_name(%s)",
                     self.cluster_id, self.service_name)
        services = objects.RPCServiceList.get_all(
            self.context,
            filters={
                "cluster_id": self.cluster_id,
                "service_name": self.service_name
            }
        )
        if not services:
            return {}
        endpoints = {v.node_id: json.loads(v.endpoint) for v in services}
        logger.debug("endpints: %s", endpoints)
        return endpoints

    def get_endpoints(self):
        if not self.endpoints:
            self.endpoints = self._get_endpoints_db()
        return self.endpoints

    def get_endpoint(self, node_id=None):
        endpoints = self.get_endpoints()
        if not node_id:
            for node_id, endpoint in endpoints.items():
                return endpoint
        if node_id not in endpoints:
            raise exception.EndpointNotFound(
                service_name=self.service_name, node_id=node_id)
        return self.endpoints[node_id]

    def get_stub(self, node_id=None):
        endpoint = self.get_endpoint(node_id)
        url = "{}:{}".format(endpoint['ip'], endpoint['port'])
        logger.debug("Try connect: %s", url)
        channel = grpc.insecure_channel(url)
        stub = stor_pb2_grpc.RPCServerStub(channel)
        return stub

    def get_client(self, node_id=None):
        return self.client_cls(self.get_stub(node_id=node_id),
                               async_support=self.async_support)

    def get_clients(self, hosts=None):
        raise NotImplementedError("get_stub not Implemented")


class BaseClients:
    _clients = None

    def __init__(self, clients=None):
        self._clients = clients

    def __item__(self, key):
        return self._clients[key]


class BaseClient(object):
    _stub = None

    def __init__(self, stub, async_support=False):
        self._stub = stub
        self.async_support = async_support
        obj_serializer = objects_base.StorObjectSerializer()
        self.serializer = RequestContextSerializer(obj_serializer)

    def call(self, context, method, version="v1.0", **kwargs):
        if self.async_support:
            return self._async_call(context, method, version, **kwargs)
        else:
            return self._sync_call(context, method, version, **kwargs)

    def _sync_call(self, context, method, version, **kwargs):
        context = self.serializer.serialize_context(context)
        kwargs = self.serializer.serialize_entity(context, kwargs)
        response = self._stub.call(stor_pb2.Request(
            context=json.dumps(context),
            method=method,
            kwargs=json.dumps(kwargs),
            version=version
        ))
        ret = self.serializer.deserialize_entity(
            context, json.loads(response.value))
        return ret

    def _fwrap(self, f, gf, context):
        try:
            response = gf.result()
            ret = self.serializer.deserialize_entity(
                context, json.loads(response.value))
            f.set_result(ret)
        except Exception as e:
            f.set_exception(e)

    def _async_call(self, context, method, version, **kwargs):
        context = self.serializer.serialize_context(context)
        kwargs = self.serializer.serialize_entity(context, kwargs)
        gf = self._stub.call.future(stor_pb2.Request(
            context=json.dumps(context),
            method=method,
            kwargs=json.dumps(kwargs),
            version=version
        ))

        f = Future()
        ioloop = IOLoop.current()

        gf.add_done_callback(
            lambda _: ioloop.add_callback(self._fwrap, f, gf, context)
        )
        return f
