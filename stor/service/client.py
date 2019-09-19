from __future__ import print_function
import logging
import json

import grpc
import etcd3

from stor.service import stor_pb2
from stor.service import stor_pb2_grpc
from stor.objects import base as objects_base
from stor.service.serializer import RequestContextSerializer


logger = logging.getLogger(__name__)


class BaseClientManager:
    cluster = None
    channel = None
    service_name = None
    endpoints = None
    client_cls = None

    def __init__(self):
        etcd = etcd3.client(host='127.0.0.1', port=2379)
        values = etcd.get_prefix(
            '/t2stor/service/{}/{}'.format(self.cluster, self.service_name))
        self.endpoints = {}
        if not values:
            raise Exception("Service {} not found".format(self.service_name))
        for value, meta in values:
            endpoint = json.loads(value)
            hostname = meta.key.split(b"/")[-1].decode("utf-8")
            logger.info("Service {} found in host {}".format(
                self.service_name, hostname))
            self.endpoints[hostname] = endpoint

    def get_endpoints(self):
        return self.endpoints

    def get_endpoint(self, host=None):
        if not host:
            for host, endpoint in self.endpoints.items():
                return endpoint
        if host not in self.endpoints:
            err = "Service {} for host {} not found"
            raise Exception(err.format(self.service_name, host))
        return self.endpoints[host]

    def get_channel(self, host=None):
        endpoint = self.get_endpoint(host)
        url = "{}:{}".format(endpoint['ip'], endpoint['port'])
        return grpc.insecure_channel(url)

    def get_stub(self, host=None):
        channel = self.get_channel(host=host)
        stub = stor_pb2_grpc.RPCServerStub(channel)
        return stub

    def get_client(self, host=None):
        return self.client_cls(self.get_stub(host=host))

    def get_clients(self, hosts=None):
        raise NotImplementedError("get_stub not Implemented")


class BaseClients:
    _clients = None

    def __init__(self, clients=None):
        self._clients = clients

    def __item__(self, key):
        return self._clients[key]


class BaseClient:
    _stub = None

    def __init__(self, stub):
        self._stub = stub
        obj_serializer = objects_base.StorObjectSerializer()
        self.serializer = RequestContextSerializer(obj_serializer)

    # def __getattr__(self, key):
    #     method = getattr(self._stub, key)
    #     return method

    def call(self, context, method, version="v1.0", **kwargs):
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
