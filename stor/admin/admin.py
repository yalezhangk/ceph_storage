from concurrent import futures
import json
import queue
import sys
import time

from oslo_log import log as logging

from stor.service import ServiceBase
from stor.agent import AgentClientManager
from stor import version
from stor import objects
from stor.common.config import CONF


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


class AdminQueue(queue.Queue):
    pass


class AdminHandler(object):
    def __init__(self):
        self.worker_queue = AdminQueue()
        self.executor = futures.ThreadPoolExecutor(max_workers=10)

    def get_ceph_conf(self, ctxt, ceph_host):
        logger.debug("try get ceph conf with location "
                     "{}".format(ceph_host))
        return example

    def volume_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                       sort_dirs=None, filters=None, offset=None):
        return objects.VolumeList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def volume_get(self, ctxt, volume_id):
        return objects.Volume.get_by_id(ctxt, volume_id)

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

    def cluster_import(self, ctxt):
        """Cluster import"""
        pass

    def cluster_new(self, ctxt):
        """Deploy a new cluster"""
        pass


class AdminService(ServiceBase):
    service_name = "admin"
    rpc_endpoint = None
    rpc_ip = "192.168.211.129"
    rpc_port = 2080

    def __init__(self):
        self.handler = AdminHandler()
        self.rpc_endpoint = json.dumps({
            "ip": self.rpc_ip,
            "port": self.rpc_port
        })
        super(AdminService, self).__init__()


def run_loop():
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        exit(0)


if __name__ == '__main__':
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "stor")
    AdminService().start()
    run_loop()
