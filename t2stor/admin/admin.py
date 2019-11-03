import time
from concurrent import futures

from oslo_log import log as logging

from t2stor import objects
from t2stor.admin.action_log import ActionLogHandler
from t2stor.admin.alert_group import AlertGroupHandler
from t2stor.admin.alert_log import AlertLogHandler
from t2stor.admin.alert_rule import AlertRuleHandler
from t2stor.admin.ceph_config import CephConfigHandler
from t2stor.admin.crush_rule import CephCrushHandler
from t2stor.admin.datacenter import DatacenterHandler
from t2stor.admin.disk import DiskHandler
from t2stor.admin.email_group import EmailGroupHandler
from t2stor.admin.mail import MailHandler
from t2stor.admin.node import NodeHandler
from t2stor.admin.osd import OsdHandler
from t2stor.admin.pool import PoolHandler
from t2stor.admin.prometheus import PrometheusHandler
from t2stor.admin.rack import RackHandler
from t2stor.admin.service import ServiceHandler
from t2stor.admin.sys_config import SysConfigHandler
from t2stor.admin.volume import VolumeHandler
from t2stor.admin.volume_access_path import VolumeAccessPathHandler
from t2stor.admin.volume_client import VolumeClientHandler
from t2stor.admin.volume_snapshot import VolumeSnapshotHandler
from t2stor.service import ServiceBase
from t2stor.taskflows.ceph import CephTask
from t2stor.taskflows.node import NodeTask
from t2stor.tools.base import Executor
from t2stor.tools.ceph import CephTool

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
logger = logging.getLogger(__name__)


class AdminHandler(ActionLogHandler,
                   AlertGroupHandler,
                   AlertLogHandler,
                   AlertRuleHandler,
                   CephConfigHandler,
                   CephCrushHandler,
                   DatacenterHandler,
                   DiskHandler,
                   EmailGroupHandler,
                   OsdHandler,
                   MailHandler,
                   NodeHandler,
                   PoolHandler,
                   PrometheusHandler,
                   RackHandler,
                   ServiceHandler,
                   SysConfigHandler,
                   VolumeAccessPathHandler,
                   VolumeHandler,
                   VolumeClientHandler,
                   VolumeSnapshotHandler):
    def __init__(self):
        self.executor = futures.ThreadPoolExecutor(max_workers=10)

    ###################

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
        task = NodeTask()
        task.t2stor_agent_install(ip_address, password)
        return True

    def network_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                        sort_dirs=None, filters=None, offset=None,
                        expected_attrs=None):
        networks = objects.NetworkList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs
        )
        return networks

    ###################

    def _update_osd_crush_id(self, ctxt, osds, crush_rule_id):
        for osd_id in osds:
            osd = self.osd_get(ctxt, osd_id)
            osd.crush_rule_id = crush_rule_id
            osd.save()

    ###################

    def ceph_cluster_info(self, ctxt):
        try:
            ceph_client = CephTask(ctxt)
            cluster_info = ceph_client.cluster_info()
        except Exception as e:
            logger.error(e)
            return {}
        fsid = cluster_info.get('fsid')
        total_cluster_byte = cluster_info.get('cluster_data', {}).get(
            'stats', {}).get('total_bytes')
        pool_list = cluster_info.get('cluster_data', {}).get('pools')
        return {
            'fsid': fsid,
            'total_cluster_byte': total_cluster_byte,
            'pool_list': pool_list
        }


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
