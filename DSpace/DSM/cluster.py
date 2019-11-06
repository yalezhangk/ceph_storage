import six
from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.objects import fields as s_fields
from DSpace.taskflows.ceph import CephTask
from DSpace.taskflows.node import NodeTask
from DSpace.tools.base import SSHExecutor
from DSpace.tools.ceph import CephTool

logger = logging.getLogger(__name__)


class ClusterHandler(AdminBaseHandler):
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

    def cluster_import(self, ctxt):
        """Cluster import"""
        pass

    def cluster_create(self, ctxt, data):
        """Deploy a new cluster"""
        cluster = objects.Cluster(
            ctxt, display_name=data.get('cluster_name'))
        cluster.create()

        ctxt.cluster_id = cluster.id
        # TODO check key value
        for key, value in six.iteritems(data):
            sysconf = objects.SysConfig(
                ctxt, key=key, value=value,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()
        return cluster

    def cluster_install_agent(self, ctxt, ip_address, password):
        logger.debug("Install agent on {}".format(ip_address))
        task = NodeTask()
        task.dspace_agent_install(ip_address, password)
        return True

    def cluster_get_info(self, ctxt, ip_address, password=None):
        logger.debug("detect an exist cluster from {}".format(ip_address))
        ssh_client = SSHExecutor()
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
