import six
from oslo_log import log as logging

from DSpace import exception as exc
from DSpace import objects
from DSpace.DSM.alert_rule import AlertRuleInitMixin
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.taskflows.ceph import CephTask
from DSpace.taskflows.node import NodeTask
from DSpace.tools.base import SSHExecutor
from DSpace.tools.ceph import CephTool

logger = logging.getLogger(__name__)


class ClusterHandler(AdminBaseHandler, AlertRuleInitMixin):
    def ceph_cluster_info(self, ctxt):
        has_mon_host = self.has_monitor_host(ctxt)
        if not has_mon_host:
            return {}
        try:
            ceph_client = CephTask(ctxt)
            cluster_info = ceph_client.cluster_info()
        except Exception as e:
            logger.exception('get cluster info error: %s', e)
            return {}
        fsid = cluster_info.get('fsid')
        total_cluster_byte = cluster_info.get('cluster_data', {}).get(
            'stats', {}).get('total_bytes')
        pool_list = cluster_info.get('cluster_data', {}).get('pools')
        logger.debug('total_cluster_byte: %s', total_cluster_byte)
        logger.debug('pool_list: %s', pool_list)
        return {
            'fsid': fsid,
            'total_cluster_byte': total_cluster_byte,
            'pool_list': pool_list
        }

    def cluster_import(self, ctxt):
        """Cluster import"""
        pass

    def check_admin_node_status(self, ctxt):
        clusters = objects.ClusterList.get_all(ctxt)
        if not len(clusters):
            return True
        for c in clusters:
            nodes = objects.NodeList.get_all(
                ctxt,
                filters={
                    "cluster_id": c.id
                }
            )
            if len(nodes):
                return False
        return True

    def _admin_node_delete(self, ctxt, node):
        try:
            node_task = NodeTask(ctxt, node)
            node_task.chrony_uninstall()
            node_task.node_exporter_uninstall()
            node_task.dspace_agent_uninstall()
            node_task.prometheus_target_config(action='remove',
                                               service='node_exporter')

            rpc_services = objects.RPCServiceList.get_all(
                ctxt,
                filters={
                    "cluster_id": node.cluster_id,
                    "service_name": "agent",
                    "node_id": node.id
                }
            )
            for rpc_service in rpc_services:
                rpc_service.destroy()
            node.destroy()
            logger.debug("Admin node removed success!")
        except Exception as e:
            node.status = s_fields.NodeStatus.ERROR
            node.save()
            logger.exception("Admin node remove error: %s", e)

    def cluster_platform_check(self, ctxt):
        """Judge platform init success"""
        logger.debug("cluster platform check")
        clusters = objects.ClusterList.get_all(ctxt)
        if not len(clusters):
            return False

        admin_ips = objects.sysconfig.sys_config_get(ctxt, "admin_ips")
        admin_ips = admin_ips.split(',')

        success = True
        cluster = clusters[0]
        ctxt.cluster_id = cluster.id
        for ip_address in admin_ips:
            nodes = objects.NodeList.get_all(
                ctxt, filters={"ip_address": ip_address}
            )
            if not len(nodes) or nodes[0].status != s_fields.NodeStatus.ACTIVE:
                success = False
        if not success:
            # clean cluster infos
            for c in clusters:
                # remove sysconfigs
                sysconfigs = objects.SysConfigList.get_all(
                    ctxt,
                    filters={
                        "cluster_id": c.id
                    }
                )
                for conf in sysconfigs:
                    conf.destroy()
                # remove alert rules
                alert_rules = objects.AlertRuleList.get_all(
                    ctxt,
                    filters={
                        "cluster_id": c.id
                    }
                )
                for rule in alert_rules:
                    rule.destroy()
                # remove networks
                networks = objects.NetworkList.get_all(
                    ctxt,
                    filters={
                        "cluster_id": c.id
                    }
                )
                for net in networks:
                    net.destroy()
                # remove disks
                disks = objects.DiskList.get_all(
                    ctxt,
                    filters={
                        "cluster_id": c.id
                    }
                )
                for disk in disks:
                    disk.destroy()
                # remove nodes
                nodes = objects.NodeList.get_all(
                    ctxt,
                    filters={
                        "cluster_id": c.id
                    }
                )
                for node in nodes:
                    logger.debug("delete admin node %s start", node.ip_address)
                    self.task_submit(self._admin_node_delete, ctxt, node)
                # remove cluster
                c.destroy()
        return success

    def cluster_admin_nodes_get(self, ctxt):
        logger.debug("get admin nodes info")
        nodes = []
        admin_ips = objects.sysconfig.sys_config_get(ctxt, "admin_ips")
        admin_ips = admin_ips.split(',')
        has_ceph = False
        for ip_address in admin_ips:
            nodes.append({"ip_address": ip_address})
            # check if ceph cluster exist
            node = objects.Node(
                ctxt, ip_address=ip_address, password=None)
            node_task = NodeTask(ctxt, node)
            if node_task.check_ceph_is_installed():
                has_ceph = True

        admin_nodes = {
            "has_ceph": has_ceph,
            "nodes": nodes
        }

        return admin_nodes

    def _cluster_create_check(self, ctxt, data):
        if data.get('admin_create'):
            clusters = objects.ClusterList.get_all(
                ctxt, filters={'is_admin': True})
            if clusters:
                raise exc.Duplicate(_("Admin cluster exists"))

        clusters = objects.ClusterList.get_all(
            ctxt, filters={'display_name': data.get('cluster_name')})
        if clusters:
            raise exc.Duplicate(_("Cluster name exists"))

    def cluster_create(self, ctxt, data):
        """Deploy a new cluster"""
        logger.debug("Create a new cluster")
        self._cluster_create_check(ctxt, data)
        cluster = objects.Cluster(
            ctxt,
            is_admin=data.get('admin_create'),
            display_name=data.get('cluster_name'))
        cluster.create()

        ctxt.cluster_id = cluster.id
        # TODO check key value
        for key, value in six.iteritems(data):
            sysconf = objects.SysConfig(
                ctxt, key=key, value=value,
                value_type=s_fields.ConfigType.STRING)
            sysconf.create()

        self.task_submit(self.init_alert_rule, ctxt, cluster.id)
        logger.info('cluster %s init alert_rule task has begin', cluster.id)
        return cluster

    def cluster_install_agent(self, ctxt, ip_address, password):
        logger.debug("Install agent on {}".format(ip_address))
        task = NodeTask()
        task.dspace_agent_install(ip_address, password)
        return True

    def cluster_get_info(self, ctxt, ip_address, password=None):
        logger.debug("detect an exist cluster from %s", ip_address)
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
            if k == s_fields.OsdStatus.AVAILABLE:
                status[s_fields.OsdStatus.AVAILABLE] = v
            elif k == s_fields.OsdStatus.INACTIVE:
                status[s_fields.OsdStatus.INACTIVE] = v
            elif k == s_fields.OsdStatus.ERROR:
                status[s_fields.OsdStatus.ERROR] = v
            elif k == s_fields.OsdStatus.INUSE:
                status[s_fields.OsdStatus.INUSE] = v
            else:
                num += v
        status["progress"] = num
        return status
