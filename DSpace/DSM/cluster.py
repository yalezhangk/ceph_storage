import six
from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.alert_rule import AlertRuleInitMixin
from DSpace.DSM.base import AdminBaseHandler
from DSpace.objects import fields as s_fields
from DSpace.taskflows.ceph import CephTask
from DSpace.taskflows.node import NodeTask
from DSpace.tools.base import SSHExecutor
from DSpace.tools.ceph import CephTool

logger = logging.getLogger(__name__)


class ClusterHandler(AdminBaseHandler, AlertRuleInitMixin):
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
        # init alert_rule
        self.executor.submit(self.init_alert_rule, ctxt, cluster.id)
        logger.info('cluster %s init alert_rule task has begin', cluster.id)
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

    def service_status_get(self, ctxt, names):
        return objects.ServiceList.service_status_get(ctxt, names=names)

    def cluster_host_status_get(self, ctxt):
        query_all = objects.NodeList.get_status(ctxt)
        num = 0
        status = {s_fields.NodeStatus.ACTIVE: 0,
                  s_fields.NodeStatus.ERROR: 0,
                  s_fields.NodeStatus.INACTIVE: 0}
        for [k, v] in query_all:
            if k in [s_fields.NodeStatus.ACTIVE,
                     s_fields.NodeStatus.ERROR,
                     s_fields.NodeStatus.INACTIVE]:
                status[k] = v
            else:
                num += v
        status["progress"] = num
        return status

    def cluster_pool_status_get(self, ctxt):
        query_all = objects.PoolList.get_status(ctxt)
        num = 0
        status = {s_fields.PoolStatus.ACTIVE: 0,
                  s_fields.PoolStatus.INACTIVE: 0,
                  s_fields.PoolStatus.ERROR: 0}
        for [k, v] in query_all:
            if k in [s_fields.PoolStatus.ACTIVE,
                     s_fields.PoolStatus.ERROR,
                     s_fields.PoolStatus.INACTIVE]:
                status[k] = v
            elif k != s_fields.PoolStatus.DELETED:
                num += v
        status["progress"] = num
        return status

    def cluster_osd_status_get(self, ctxt):
        query_all = objects.OsdList.get_status(ctxt)
        num = 0
        status = {s_fields.OsdStatus.ACTIVE: 0,
                  s_fields.OsdStatus.ERROR: 0,
                  s_fields.OsdStatus.INACTIVE: 0,
                  s_fields.OsdStatus.INUSE: 0}
        for [k, v] in query_all:
            if k == s_fields.OsdStatus.UP:
                status[s_fields.OsdStatus.ACTIVE] = v
            elif k == s_fields.OsdStatus.DOWN:
                status[s_fields.OsdStatus.INACTIVE] = v
            elif k == s_fields.OsdStatus.ERROR:
                status[s_fields.OsdStatus.ERROR] = v
            elif k == s_fields.OsdStatus.INUSE:
                status[s_fields.OsdStatus.INUSE] = v
            else:
                num += v
        status["progress"] = num
        return status
