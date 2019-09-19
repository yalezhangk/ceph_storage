from concurrent import futures
import time
import logging
import threading
import socket
import json

import etcd3
import grpc

from stor.service import stor_pb2
from stor.service import stor_pb2_grpc
from stor import exception
from stor.objects import base as objects_base
from stor.service.serializer import RequestContextSerializer


logger = logging.getLogger(__name__)
_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class RPCService(stor_pb2_grpc.RPCServerServicer):
    def __init__(self, api, *args, **kwargs):
        super(RPCService, self).__init__(*args, **kwargs)
        self.api = api
        obj_serializer = objects_base.StorObjectSerializer()
        self.serializer = RequestContextSerializer(obj_serializer)

    def call(self, request, context):
        logger.debug("get rpc call: ctxt(%s), method(%s), kwargs(%s),"
                     " version(%s)" % (
                         request.context,
                         request.method,
                         request.kwargs,
                         request.version,
                     ))
        method = request.method
        ctxt = self.serializer.deserialize_context(json.loads(request.context))
        kwargs = self.serializer.deserialize_entity(
            ctxt, json.loads(request.kwargs))
        if not hasattr(self.api, method):
            raise exception.NoSuchMethod(method=method)
        func = getattr(self.api, method)
        try:
            ret = func(ctxt, **kwargs)
            logger.debug("%s ret: %s" % (
                func.__name__,
                json.dumps(
                    self.serializer.serialize_entity(ctxt, ret))))
            res = stor_pb2.Response(
                value=json.dumps(self.serializer.serialize_entity(ctxt, ret))
            )
        except Exception as e:
            raise e
            logger.exception(e)
            res = stor_pb2.Response(
                value=self.serializer.serialize_entity(ctxt, {
                    "exception": str(e)
                })
            )
        return res


class ServiceBase:
    name = None
    cluster = None
    hostname = None
    rpc_endpoint = None
    rpc_ip = None
    rpc_port = None
    service = None
    api = None

    def __init__(self):
        if not self.hostname:
            self.hostname = socket.gethostname()

    def start_rpc(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        stor_pb2_grpc.add_RPCServerServicer_to_server(
            RPCService(self.api), server)
        server.add_insecure_port('{}:{}'.format(self.rpc_ip, self.rpc_port))
        server.start()
        self.server = server

    def stop_rpc(self):
        self.server.stop(0)

    def register_endpoint(self):
        etcd = etcd3.client(host='127.0.0.1', port=2379)
        lease = None

        while True:
            if not lease:
                lease = etcd.lease(ttl=3)
                etcd.put(
                    '/t2stor/service/{}/{}/{}'.format(
                        self.cluster,
                        self.service_name, self.hostname),
                    self.rpc_endpoint, lease=lease)
            time.sleep(2)
            res = lease.refresh()[0]
            if res.TTL == 0:
                lease = None
            logger.debug("lease refresh")

    def start_heartbeat(self):
        thread = threading.Thread(target=self.register_endpoint, args=())
        thread.daemon = True
        thread.start()

    def start(self):
        self.start_rpc()
        self.start_heartbeat()

    def stop(self):
        self.stop_rpc()

    def run(self):
        self.start()
        try:
            while True:
                time.sleep(_ONE_DAY_IN_SECONDS)
        except KeyboardInterrupt:
            self.stop()
