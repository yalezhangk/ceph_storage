import json
import logging
import sys
from concurrent import futures

import grpc
from oslo_config import cfg

from DSpace import exception
from DSpace import objects
from DSpace.common.config import CONF
from DSpace.objects import base as objects_base
from DSpace.service import stor_pb2
from DSpace.service import stor_pb2_grpc
from DSpace.service.serializer import RequestContextSerializer

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
        self.debug_mode = kwargs.get('debug_mode', False)

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
            logger.exception("%s raise exception: %s" % (
                func.__name__, e
            ))
            if self.debug_mode:
                sys.exit(1)
            res = stor_pb2.Response(
                value=json.dumps(self.serializer.serialize_exception(ctxt, e))
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
        self.rpc_endpoint = {
            "ip": self.rpc_ip,
            "port": self.rpc_port
        }
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

    def start(self):
        self.start_rpc()

    def stop(self):
        self.stop_rpc()
