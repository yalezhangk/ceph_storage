from __future__ import print_function

import json
import logging

import grpc
from tornado.gen import Future
from tornado.ioloop import IOLoop

from DSpace import exception
from DSpace import objects
from DSpace.grpc import stor_pb2
from DSpace.grpc import stor_pb2_grpc
from DSpace.objects import base as objects_base
from DSpace.service.serializer import RequestContextSerializer

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

    def __init__(self, context, cluster_id=None, async_support=False,
                 endpoints=None):
        self.context = context
        self.async_support = async_support
        if cluster_id:
            self.cluster_id = cluster_id
        if endpoints:
            self.endpoints = endpoints
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
        endpoints = {
            v.node_id: "%s:%s" % (v.endpoint['ip'], v.endpoint['port'])
            for v in services
        }
        logger.debug("endpints: %s", endpoints)
        return endpoints

    def get_endpoints(self):
        if self.endpoints:
            return self.endpoints
        return self._get_endpoints_db()

    def get_endpoint(self, node_id=None):
        endpoints = self.get_endpoints()
        if not node_id:
            for node_id, endpoint in endpoints.items():
                return endpoint
        if node_id not in endpoints:
            raise exception.EndpointNotFound(
                service_name=self.service_name, node_id=node_id)
        return endpoints[node_id]

    def get_client(self):
        endpoint = self.get_endpoint()
        client = self.client_cls(endpoint, self.async_support)
        return client


class RPCClient(object):
    """RPC Client Manager"""
    _stub = None

    def __init__(self, endpoint=None, async_support=False):
        self.async_support = async_support
        obj_serializer = objects_base.StorObjectSerializer()
        self.serializer = RequestContextSerializer(obj_serializer)
        self._stub = self.get_stub(endpoint)
        self.endpoint = endpoint

    def get_stub(self, endpoint):
        logger.debug("Try connect: %s", endpoint)
        channel = grpc.insecure_channel(endpoint)
        stub = stor_pb2_grpc.RPCServerStub(channel)
        return stub

    def __getattr__(self, name):
        def _wapper(ctxt, *args, **kwargs):
            return self.call(ctxt, name, *args, **kwargs)
        return _wapper

    def call(self, context, method, *args, **kwargs):
        logger.info("endpoint(%s) method(%s) args(%s) kwargs(%s)",
                    self.endpoint, method, args, kwargs)
        if self.async_support:
            return self._async_call(context, method, *args, **kwargs)
        else:
            return self._sync_call(context, method, *args, **kwargs)

    def _sync_call(self, context, method, *args, **kwargs):
        try:
            _context = self.serializer.serialize_context(context)
            _args = self.serializer.serialize_entity(context, args)
            _kwargs = self.serializer.serialize_entity(context, kwargs)
            response = self._stub.call(stor_pb2.Request(
                context=json.dumps(_context),
                method=method,
                args=json.dumps(_args),
                kwargs=json.dumps(_kwargs),
                version="v1.0"
            ))
        except grpc.RpcError as e:
            logger.exception("rpc connect error: %s", e)
            raise exception.RPCConnectError()
        res = json.loads(response.value)
        # check redirect
        if isinstance(res, dict) and res.get('__type__') == "Redirect":
            logger.info("Redirect to %s", res['endpoint'])
            self._stub = self.get_stub(res['endpoint'])
            return self._sync_call(context, method, *args, **kwargs)
        self.serializer.deserialize_exception(context, res)
        ret = self.serializer.deserialize_entity(
            context, res)
        return ret

    def _fwrap(self, future, gf, context, method, *args, **kwargs):
        try:
            response = gf.result()
            res = json.loads(response.value)
            # check redirect
            if isinstance(res, dict) and res.get('__type__') == "Redirect":
                logger.info("Redirect to %s", res['endpoint'])
                self._stub = self.get_stub(res['endpoint'])
                _f = self._async_call(context, method, *args, **kwargs)
                _f.add_done_callback(lambda f: future.set_result(f.result()))
            else:
                self.serializer.deserialize_exception(context, res)
                ret = self.serializer.deserialize_entity(
                    context, res)
                future.set_result(ret)
        except Exception as e:
            future.set_exception(e)

    def _async_call(self, context, method, *args, **kwargs):
        try:
            _context = self.serializer.serialize_context(context)
            _args = self.serializer.serialize_entity(context, args)
            _kwargs = self.serializer.serialize_entity(context, kwargs)
            gf = self._stub.call.future(stor_pb2.Request(
                context=json.dumps(_context),
                method=method,
                args=json.dumps(_args),
                kwargs=json.dumps(_kwargs),
                version="v1.0"
            ))
        except grpc.RpcError as e:
            logger.exception("rpc connect error: %s", e)
            raise exception.RPCConnectError()

        f = Future()
        ioloop = IOLoop.current()

        gf.add_done_callback(
            lambda _: ioloop.add_callback(
                self._fwrap, f, gf, context, method, *args, **kwargs)
        )
        return f
