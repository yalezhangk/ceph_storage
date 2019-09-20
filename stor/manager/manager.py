from concurrent import futures
import logging
import json
import queue

from stor.service import ServiceBase
from stor.agent import AgentClientManager


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


class ManagerQueue(queue.Queue):
    pass


class ManagerAPI(object):
    def __init__(self):
        self.worker_queue = ManagerQueue()
        self.executor = futures.ThreadPoolExecutor(max_workers=10)

    def get_ceph_conf(self, request, context):
        logger.debug("try get ceph conf with location "
                     "{}".format(request.location))
        return example

    def _append_ceph_monitor(self, ceph_monitor_host=None):
        agent = AgentClientManager()
        agent.get_client("whx-ceph-1").start_service()
        agent.get_client("whx-ceph-1").write_ceph_conf()

    def append_ceph_monitor(self, request, context):
        logger.debug("try append ceph monitor")
        self.executor.submit(
            self._append_ceph_monitor,
            ceph_monitor_host="whx-ceph-1"
        )
        return "Apply"


class ManagerService(ServiceBase):
    service_name = "manager"
    rpc_endpoint = None
    rpc_ip = "192.168.211.129"
    rpc_port = 2080

    def __init__(self):
        super(ManagerService, self).__init__()
        self.api = ManagerAPI()
        self.rpc_endpoint = json.dumps({
            "ip": self.rpc_ip,
            "port": self.rpc_port
        })


if __name__ == '__main__':
    logging.basicConfig(
        level='DEBUG',
        format='%(asctime)s :: %(levelname)s :: %(message)s')
    ManagerService().run()
