import json
import logging
import os
import time
from concurrent import futures
from enum import Enum

import etcd3
import grpc
from oslo_config import cfg

from DSpace import exception
from DSpace import objects
from DSpace.common.config import CONF
from DSpace.grpc import stor_pb2
from DSpace.grpc import stor_pb2_grpc
from DSpace.objects import base as objects_base
from DSpace.service.serializer import RequestContextSerializer
from DSpace.utils import retry

logger = logging.getLogger(__name__)

cluster_opts = [
    cfg.StrOpt('cluster_id',
               default='',
               help='Cluster ID'),
]

CONF.register_opts(cluster_opts)


class Role(Enum):
    Master = 1
    Backup = 2


class Dispatcher(stor_pb2_grpc.RPCServerServicer):
    def __init__(self, service, handler, *args, **kwargs):
        super(Dispatcher, self).__init__(*args, **kwargs)
        self.handler = handler
        obj_serializer = objects_base.StorObjectSerializer()
        self.serializer = RequestContextSerializer(obj_serializer)
        self.debug_mode = kwargs.get('debug_mode', False)
        self.service = service

    def call(self, request, context):
        logger.debug("get rpc call: method(%s), args(%s), "
                     "kwargs(%s), ctxt(%s), version(%s)" % (
                         request.method,
                         request.args,
                         request.kwargs,
                         request.context,
                         request.version,
                     ))
        method = request.method
        ctxt = self.serializer.deserialize_context(json.loads(request.context))
        args = self.serializer.deserialize_entity(
            ctxt, json.loads(request.args))
        kwargs = self.serializer.deserialize_entity(
            ctxt, json.loads(request.kwargs))
        # check is master
        if self.service.role != Role.Master:
            logger.info("Redirect rpc to: %s", self.service.master_endpoint)
            res = stor_pb2.Response(
                value=json.dumps(self.serializer.serialize_entity(ctxt, {
                    "__type__": "Redirect",
                    "endpoint": self.service.master_endpoint
                }))
            )
            return res
        # check method exists
        if not hasattr(self.handler, method):
            raise exception.NoSuchMethod(method=method)
        # run method
        func = getattr(self.handler, method)
        try:
            ret = func(ctxt, *args, **kwargs)
            logger.debug("%s ret: %s" % (
                func.__name__,
                json.dumps(
                    self.serializer.serialize_entity(ctxt, ret))))
            res = stor_pb2.Response(
                value=json.dumps(self.serializer.serialize_entity(ctxt, ret))
            )
        except Exception as e:
            code = getattr(e, 'code', 500)
            if isinstance(e, exception.StorException) and code < 500:
                # exception content will auto add to log
                logger.warning("%s", func.__name__)
            else:
                logger.exception("%s raise exception: %s" % (
                    func.__name__, e
                ))
                if self.debug_mode:
                    os._exit(1)
            res = stor_pb2.Response(
                value=json.dumps(self.serializer.serialize_exception(ctxt, e))
            )
        return res


class ServiceBase(object):
    role = None
    handler = None
    endpoint = None
    _executor = None

    def __init__(self, rpc_ip=None, rpc_port=None):
        objects.register_all()
        self.role = Role.Master
        self.endpoint = '{}:{}'.format(rpc_ip, rpc_port)
        logger.info("RPC endpoint: %s", self.endpoint)
        self._executor = self.init_threadpool()

    def init_threadpool(self):
        return futures.ThreadPoolExecutor(
            max_workers=CONF.task_workers)

    def start_rpc(self):
        server = grpc.server(self._executor)
        stor_pb2_grpc.add_RPCServerServicer_to_server(
            Dispatcher(self, self.handler), server)
        server.add_insecure_port(self.endpoint)
        server.start()
        self.server = server
        logger.debug("RPC server started on %s", self.endpoint)

    def stop_rpc(self):
        self.server.stop(0)

    def start(self):
        self.start_rpc()

    def stop(self):
        self.stop_rpc()


class ServiceCell(ServiceBase):
    master_endpoint = None
    etcd_master_key = "/dspace/dsm_master"

    def __init__(self, *args, **kwargs):
        super(ServiceCell, self).__init__(*args, **kwargs)
        self.role = Role.Backup
        self.etcd = self.init_etcd()

    def _wapper(self, fun, *args, **kwargs):
        try:
            fun(*args, **kwargs)
        except Exception as e:
            logger.exception("Unexpected exception: %s", e)

    def task_submit(self, fun, *args, **kwargs):
        self._executor.submit(self._wapper, fun, *args, **kwargs)

    def init_etcd(self):
        logger.info("etcd endpoint: %s:%s", CONF.etcd.host, CONF.etcd.port)
        etcd = etcd3.client(host=CONF.etcd.host, port=CONF.etcd.port)
        return etcd

    def show_master(self):
        while True:
            if self.role == Role.Master:
                logger.info("I am master %s", self.master_endpoint)
            else:
                logger.info("Master is %s", self.master_endpoint)
            time.sleep(10)

    def _watch_master(self):
        # watch key
        events_iterator, cancel = self.etcd.watch(self.etcd_master_key)
        # get current
        value = self.etcd_get(self.etcd_master_key)
        self.master_endpoint = value
        if not self.master_endpoint:
            self.task_submit(self.register)
        logger.info("Master endpoint current is: %s", self.master_endpoint)
        # begin to register
        # wait watch
        for event in events_iterator:
            self.master_endpoint = event.value.decode('utf-8')
            logger.info("Master endpoint change to: %s",
                        self.master_endpoint)
            if self.master_endpoint == self.endpoint:
                self.to_master()
            elif self.role == Role.Master:
                logger.error("I lost master, exit...")
                os._exit(1)
            elif not self.master_endpoint:
                self.task_submit(self.register)

    def watch_master(self):
        while True:
            try:
                self._watch_master()
            except etcd3.exceptions.ConnectionFailedError as e:
                logger.warning("ectd error: %s", e)

    def clear_etcd_key(self):
        value = self.etcd_get(self.etcd_master_key)
        if value == self.endpoint:
            self.etcd.delete(self.etcd_master_key)

    def etcd_get(self, key):
        value = self.etcd.get(self.etcd_master_key)[0]
        value = value.decode('utf-8') if value else None
        return value

    def start(self):
        # clear dirty key
        self.clear_etcd_key()
        # log master
        self.task_submit(self.show_master)
        # watch master
        self.task_submit(self.watch_master)
        super(ServiceCell, self).start()

    def to_master(self):
        logger.info("I am to master")
        self.role = Role.Master
        self.handler.bootstrap()

    def lease_refresh(self, lease):
        logger.info("Start lease refresh")

        # try 3 times
        @retry(etcd3.exceptions.ConnectionFailedError)
        def _refrese():
            return lease.refresh()[0]

        i = 0
        while True:
            try:
                i = i + 1
                logger.debug("lease refresh %s times", i)
                r = _refrese()
                if not r.TTL:
                    break
            except Exception as e:
                logger.error("lease refresh exception. %s", e)
                break
            time.sleep(3)
        logger.error("lease refresh failed. exit...")
        os._exit(1)

    @retry(etcd3.exceptions.ConnectionFailedError)
    def register(self):
        logger.info("register service")
        lease = self.etcd.lease(30)
        r = self.etcd.put_if_not_exists(
            self.etcd_master_key, self.endpoint, lease)

        if not r:
            logger.info("Master existed...")
            return
        # lease refresh
        logger.info("Master is Me")
        self.task_submit(self.lease_refresh, lease)
