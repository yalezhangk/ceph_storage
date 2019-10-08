from concurrent import futures
import json
import queue
import sys
import time

from oslo_log import log as logging

from t2stor.service import ServiceBase
from t2stor.agent import AgentClientManager
from t2stor import version
from t2stor import objects
from t2stor.common.config import CONF
from t2stor.tools.base import Executor
from t2stor.tools.ceph import Ceph as CephTool
from t2stor.tools.package import Package as PackageTool
from t2stor.tools.service import Service as ServiceTool
from t2stor.tools.service import Docker as DockerTool


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

    def cluster_import(self, ctxt):
        """Cluster import"""
        pass

    def cluster_new(self, ctxt):
        """Deploy a new cluster"""
        pass

    def cluster_get_info(self, ip_address, password=None):
        logger.debug("detect an exist cluster from {}".format(ip_address))
        ssh_client = Executor()
        ssh_client.connect(hostname=ip_address, password=password)
        tool = CephTool(ssh_client)
        cluster_info = {}
        mon_hosts = tool.get_mons()
        osd_hosts = tool.get_osds()
        mgr_hosts = tool.get_mgrs()

        cluster_info.update({'mon_hosts': mon_hosts,
                             'osd_hosts': osd_hosts,
                             'mgr_hosts': mgr_hosts})
        return cluster_info

    def cluster_install_agent(self, ip_address, password):
        logger.debug("Install agent on {}".format(ip_address))
        ssh_client = Executor()
        ssh_client.connect(hostname=ip_address, password=password)
        package_tool = PackageTool(ssh_client)
        package_tool.install(["docker-ce", "docker-ce-cli", "containerd.io"])
        service_tool = ServiceTool(ssh_client)
        service_tool.start('docker')
        docker_tool = DockerTool(ssh_client)
        docker_tool.load("/opt/t2stor/repo/files/t2stor.tar")
        docker_tool.start(
            image="t2stor/t2stor:v2.3",
            name="t2stor_portal",
            volume=[("/etc/t2stor", "/etc/t2stor")]
        )


class AdminService(ServiceBase):
    service_name = "admin"

    def __init__(self):
        self.handler = AdminHandler()
        super(AdminService, self).__init__()


def run_loop():
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        exit(0)


def service():
    admin = AdminService()
    admin.start()
    run_loop()
    admin.stop()