from concurrent import futures
import time
import logging
import threading
import socket
import json

import etcd3
import grpc
from oslo_config import cfg

from t2stor.service import stor_pb2
from t2stor.service import stor_pb2_grpc
from t2stor import exception
from t2stor import objects
from t2stor.common.config import CONF
from t2stor.objects import base as objects_base
from t2stor.service.serializer import RequestContextSerializer


logger = logging.getLogger(__name__)

cluster_opts = [
    cfg.StrOpt('cluster_id',
               default='',
               help='Cluster ID'),
]

CONF.register_opts(cluster_opts)


class RPCHandler(stor_pb2_grpc.RPCServerServicer):
    def __init__(self, handler, *args, **kwargs):
        super(RPCHandler, self).__init__(*args, **kwargs)
        self.handler = handler
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
        if not hasattr(self.handler, method):
            raise exception.NoSuchMethod(method=method)
        func = getattr(self.handler, method)
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


class ServiceBase(object):
    name = None
    cluster_id = None
    hostname = None
    service = None
    handler = None

    def __init__(self, hostname=None, rpc_ip=None, rpc_port=None):
        objects.register_all()
        self.hostname = hostname if hostname else CONF.hostname
        self.cluster_id = CONF.cluster_id
        self.rpc_ip = rpc_ip if rpc_ip else CONF.my_ip
        port_conf = "{}_port".format(self.service_name)
        self.rpc_port = rpc_port if rpc_port else getattr(CONF, port_conf)
        self.rpc_endpoint = json.dumps({
            "ip": self.rpc_ip,
            "port": self.rpc_port
        })
        logger.debug(self.rpc_endpoint)

    def start_rpc(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        stor_pb2_grpc.add_RPCServerServicer_to_server(
            RPCHandler(self.handler), server)
        port = '{}:{}'.format(self.rpc_ip, self.rpc_port)
        server.add_insecure_port(port)
        server.start()
        self.server = server
        logger.debug("RPC server started on %s", port)

    def stop_rpc(self):
        self.server.stop(0)

    def _register_endpoint(self):
        etcd = etcd3.client(host='127.0.0.1', port=2379)
        lease = etcd.lease(ttl=3)
        logger.debug("etcd server(%s) cluster_id(%s) service_name(%s)" % (
            "127.0.0.1", self.cluster_id, self.service_name
        ))
        etcd.put(
            '/t2stor/service/{}/{}/{}'.format(
                self.cluster_id,
                self.service_name, self.hostname),
            self.rpc_endpoint, lease=lease)
        return lease

    def register_endpoint(self):
        lease = None

        while True:
            if not lease:
                lease = self._register_endpoint()
            time.sleep(2)
            res = lease.refresh()[0]
            if res.TTL == 0:
                lease = None
            # logger.debug("lease refresh")

    def start_heartbeat(self):
        thread = threading.Thread(target=self.register_endpoint, args=())
        thread.daemon = True
        thread.start()

    def start(self):
        self.start_rpc()
        # self.start_heartbeat()

    def stop(self):
        self.stop_rpc()
