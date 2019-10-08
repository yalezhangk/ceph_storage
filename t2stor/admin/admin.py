from concurrent import futures
import json
import queue
import sys
import time

from oslo_log import log as logging

from t2stor.service import ServiceBase
from t2stor.agent import AgentClientManager
from t2stor.admin.genconf import ceph_conf
from t2stor.admin.genconf import yum_repo
from t2stor.admin.genconf import get_agent_conf
from t2stor import version
from t2stor import objects
from t2stor.common.config import CONF
from t2stor.tools.base import Executor
from t2stor.tools.ceph import Ceph as CephTool
from t2stor.tools.package import Package as PackageTool
from t2stor.tools.service import Service as ServiceTool
from t2stor.tools.docker import Docker as DockerTool
from t2stor.tools.file import File as FileTool


_ONE_DAY_IN_SECONDS = 60 * 60 * 24
logger = logging.getLogger(__name__)


class AdminQueue(queue.Queue):
    pass


class AdminHandler(object):
    def __init__(self):
        self.worker_queue = AdminQueue()
        self.executor = futures.ThreadPoolExecutor(max_workers=10)

    def get_ceph_conf(self, ctxt, ceph_host):
        logger.debug("try get ceph conf with location "
                     "{}".format(ceph_host))
        return ceph_conf

    def volume_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                       sort_dirs=None, filters=None, offset=None):
        filters = filters or {}
        filters['cluster_id'] = ctxt.cluster_id
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

    def cluster_get_info(self, ctxt, ip_address, password=None):
        logger.debug("detect an exist cluster from {}".format(ip_address))
        ssh_client = Executor()
        ssh_client.connect(hostname=ip_address, password=password)
        tool = CephTool(ssh_client)
        cluster_info = {}
        mon_hosts = tool.get_mons()
        osd_hosts = tool.get_osds()
        mgr_hosts = tool.get_mgrs()
        cluster_network, public_network = tool.get_networks()

        cluster_info.update({'mon_hosts': mon_hosts,
                             'osd_hosts': osd_hosts,
                             'mgr_hosts': mgr_hosts,
                             'public_network': str(public_network),
                             'cluster_network': str(cluster_network)})
        return cluster_info

    def cluster_install_agent(self, ctxt, ip_address, password):
        logger.debug("Install agent on {}".format(ip_address))
        ssh_client = Executor()
        ssh_client.connect(hostname=ip_address, password=password)
        # create config
        file_tool = FileTool(ssh_client)
        file_tool.mkdir("/etc/t2stor")
        file_tool.write("/etc/t2stor/agent.conf", get_agent_conf(ip_address))
        file_tool.write("/etc/yum.repos.d/yum.repo", yum_repo)
        # install docker
        package_tool = PackageTool(ssh_client)
        package_tool.install(["docker-ce", "docker-ce-cli", "containerd.io"])
        # start docker
        service_tool = ServiceTool(ssh_client)
        service_tool.start('docker')
        # load image
        docker_tool = DockerTool(ssh_client)
        docker_tool.image_load("/opt/t2stor/repo/files/t2stor.tar")
        # run container
        docker_tool.run(
            image="t2stor/t2stor:v2.3",
            command="agent",
            name="t2stor_portal",
            volumes=[("/etc/t2stor", "/etc/t2stor")]
        )
        return True


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
