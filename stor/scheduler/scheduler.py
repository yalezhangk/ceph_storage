from concurrent import futures
import time
import logging
import json
import threading
import queue
import inspect

import grpc

from . import scheduler_pb2
from . import scheduler_pb2_grpc
from ..service import ServiceBase
from ..agent import AgentClientManager


_ONE_DAY_IN_SECONDS = 60 * 60 * 24
logger = logging.getLogger(__name__)


example = """
[global]

fsid = 149e7202-cac3-4181-bbb1-66fea2ca3be2
mon initial members = whx-ceph-1
mon host = 172.159.4.11
public network =  172.159.0.0/16
cluster network = 172.159.0.0/16
auth cluster required = cephx
auth service required = cephx
auth client required = cephx
osd journal size = 1024
osd pool default size = 3
osd pool default min size = 2
osd pool default pg num = 333
osd pool default pgp num = 333
osd crush chooseleaf type = 1
"""

class SchedulerQueue(queue.Queue):
    pass


worker_queue = SchedulerQueue()


class SchedulerTasks:
    def append_ceph_monitor(self, ceph_monitor_host=None):
        agent = AgentClientManager()
        agent.get_client("whx-ceph-1").start_service()
        agent.get_client("whx-ceph-1").write_ceph_conf()

    def get_task_maps(self):
        members = inspect.getmembers(self, predicate=inspect.ismethod)
        maps = {}
        for name, method in members:
            if name.startswith("_"):
                continue
            maps[name] = method
        return maps




class SchedulerRpcService(scheduler_pb2_grpc.SchedulerServicer):

    def GetCephConf(self, request, context):
        logger.debug("try get ceph conf with location "
                     "{}".format(request.location))
        return scheduler_pb2.CephConf(content=example, location=request.location)

    def AppendCephMonitor(self, request, context):
        logger.debug("try append ceph monitor")
        worker_queue.put({
            "action": "append_ceph_monitor",
            "args": {
                "ceph_monitor_host": "whx-ceph-1"
            }
        })
        return scheduler_pb2.SResponse(value="Success")


class SchedulerService(ServiceBase):
    service_name = "scheduler"
    rpc_endpoint = None
    rpc_ip = "172.159.4.11"
    rpc_port = 50051
    executor = None

    def __init__(self):
        super(SchedulerService, self).__init__()
        self.rpc_endpoint = json.dumps({
            "ip": self.rpc_ip,
            "port": self.rpc_port
        })

    def start_workers(self):
        self.executor = futures.ThreadPoolExecutor(max_workers=10)
        
    def rpc_register_service(self, server):
        scheduler_pb2_grpc.add_SchedulerServicer_to_server(SchedulerRpcService(), server)

    def run(self):
        self.start_workers()
        self.start()
        maps = SchedulerTasks().get_task_maps()
        try:
            while True:
                item = worker_queue.get()
                if item['action'] in maps:
                    self.executor.submit(maps[item['action']], **item['args'])
                else:
                    raise NotImplementedError("action=%s" % item['action'])

        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    logging.basicConfig(
        level='DEBUG',
        format='%(asctime)s :: %(levelname)s :: %(message)s')
    SchedulerService().run()
