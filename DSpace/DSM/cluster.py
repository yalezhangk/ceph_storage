import six
from oslo_log import log as logging

from DSpace import exception as exc
from DSpace import objects
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.DSM.alert_rule import AlertRuleInitMixin
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.taskflows.ceph import CephTask
from DSpace.taskflows.cluster import cluster_delete_flow
from DSpace.taskflows.node import NodeTask
from DSpace.tools.base import SSHExecutor
from DSpace.tools.ceph import CephTool
from DSpace.tools.prometheus import PrometheusTool

logger = logging.getLogger(__name__)


class ClusterHandler(AdminBaseHandler, AlertRuleInitMixin):

    def cluster_get(self, ctxt, cluster_id):
        cluster = objects.Cluster.get_by_id(ctxt, cluster_id)
        return cluster

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
            status=s_fields.ClusterStatus.ACTIVE,
            display_name=data.get('cluster_name'))
        cluster.create()

        ctxt.cluster_id = cluster.id
        begin_action = self.begin_action(ctxt, Resource.CLUSTER, Action.CREATE)
        # TODO check key value
        for key, value in six.iteritems(data):
            sysconf = objects.SysConfig(
                ctxt, key=key, value=value,
                value_type=s_fields.ConfigType.STRING)
            sysconf.create()

        self.task_submit(self.init_alert_rule, ctxt, cluster.id)
        self.finish_action(begin_action, cluster.id, cluster.display_name,
                           objects.json_encode(cluster))
        logger.info('cluster %s init alert_rule task has begin', cluster.id)
        return cluster

    def _cluster_delete(self, ctxt, cluster, src_cluster_id, clean_ceph=False,
                        begin_action=None):
        logger.info("trying to delete cluster-%s", cluster.id)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        try:
            t = objects.Task(
                ctxt,
                name="Delete Cluster",
                description="Delete Cluster",
                current="",
                step_num=0,
                status=s_fields.TaskStatus.RUNNING,
                step=0
            )
            t.create()
            cluster_delete_flow(ctxt, t, clean_ceph)
            cluster.destroy()
            msg = _("Cluster delete success")
            action = "DELETE_CLUSTER_SUCCESS"
            logger.info("delete cluster-%s success", cluster.id)
            status = 'success'
            err_msg = None
        except Exception as e:
            logger.exception("delete cluster-%s error: %s", cluster.id, e)
            status = s_fields.ClusterStatus.ERROR
            cluster.status = status
            cluster.save()
            msg = _("Cluster delete error!")
            action = "DELETE_CLUSTER_ERROR"
            err_msg = str(e)

        self.finish_action(begin_action, cluster.id, cluster.display_name,
                           objects.json_encode(cluster), status,
                           err_msg=err_msg)
        ctxt.cluster_id = src_cluster_id
        logger.debug("delete cluster %s finish: %s", cluster.id, msg)
        wb_client.send_message(ctxt, cluster, action, msg)

    def _cluster_delete_check(self, ctxt, cluster):
        if cluster.is_admin:
            raise exc.InvalidInput(_('Admin cluster cannot be delete'))
        if cluster.status not in [s_fields.ClusterStatus.ACTIVE,
                                  s_fields.ClusterStatus.ERROR]:
            raise exc.InvalidInput(_('Cluster is %s') % cluster.status)
        # check if there are any nodes doing task
        nodes = objects.NodeList.get_all(
            ctxt, filters={
                "status": [
                    s_fields.NodeStatus.CREATING,
                    s_fields.NodeStatus.DELETING,
                    s_fields.NodeStatus.DEPLOYING_ROLE,
                    s_fields.NodeStatus.REMOVING_ROLE
                ]
            }
        )
        if len(nodes):
            raise exc.InvalidInput(_('Cluster has nodes in doing task'))
        osds = objects.OsdList.get_all(
            ctxt, filters={
                "status": [
                    s_fields.OsdStatus.CREATING,
                    s_fields.OsdStatus.DELETING
                ]
            }
        )
        if len(osds):
            raise exc.InvalidInput(_('Cluster has osds in doing task'))
        import_task = objects.sysconfig.sys_config_get(ctxt, "import_task_id")
        if import_task is not None and import_task >= 0:
            raise exc.InvalidInput(_('Cluster is importing'))

    def cluster_delete(self, ctxt, cluster_id, clean_ceph=False):
        logger.debug("delete cluster %s start", cluster_id)
        src_cluster_id = ctxt.cluster_id
        ctxt.cluster_id = cluster_id
        cluster = objects.Cluster.get_by_id(ctxt, cluster_id)
        self._cluster_delete_check(ctxt, cluster)
        begin_action = self.begin_action(ctxt, Resource.CLUSTER, Action.DELETE)
        cluster.status = s_fields.ClusterStatus.DELETING
        cluster.save()
        self.task_submit(self._cluster_delete,
                         ctxt,
                         cluster,
                         src_cluster_id,
                         clean_ceph=clean_ceph, begin_action=begin_action)
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
                  s_fields.OsdStatus.AVAILABLE: 0}
        for [k, v] in query_all:
            if k == s_fields.OsdStatus.AVAILABLE:
                status[s_fields.OsdStatus.AVAILABLE] = v
            elif k == s_fields.OsdStatus.INACTIVE:
                status[s_fields.OsdStatus.INACTIVE] = v
            elif k == s_fields.OsdStatus.ERROR:
                status[s_fields.OsdStatus.ERROR] = v
            elif k == s_fields.OsdStatus.ACTIVE:
                status[s_fields.OsdStatus.ACTIVE] = v
            else:
                num += v
        status["progress"] = num
        return status

    def cluster_capacity_status_get(self, ctxt):
        has_mon_host = self.has_monitor_host(ctxt)
        if has_mon_host:
            ceph_client = CephTask(ctxt)
            capacity = ceph_client.get_ceph_df()
        else:
            capacity = None
        return capacity

    def cluster_pg_status_get(self, ctxt):
        _pools = objects.PoolList.get_all(ctxt)
        pg_states = {}
        pg_states["pools"] = []
        prometheus = PrometheusTool(ctxt)
        total = {
            "total": {
                "healthy": 0,
                "recovering": 0,
                "degraded": 0,
                "unactive": 0
            }
        }
        len_pool = len(_pools)
        for pool in _pools:
            pool.metrics = {}
            prometheus.pool_get_pg_state(pool)
            pg_state = pool.metrics.get("pg_state")
            pg_state.update({
                "pool_id": pool.pool_id,
                "id": pool.id,
                "display_name": pool.display_name})
            pg_states["pools"].append(pg_state)
            total["total"]["healthy"] += pg_state["healthy"] / len_pool
            total["total"]["recovering"] += pg_state["recovering"] / len_pool
            total["total"]["degraded"] += pg_state["degraded"] / len_pool
            total["total"]["unactive"] += pg_state["unactive"] / len_pool
        pg_states.update(total)
        return pg_states

    def cluster_switch(self, ctxt, cluster_id):
        user_id = ctxt.user_id
        user = objects.User.get_by_id(ctxt, user_id)
        user.current_cluster_id = cluster_id
        user.save()
        return cluster_id

    def cluster_data_balance_get(self, ctxt):
        has_mon_host = self.has_monitor_host(ctxt)
        if has_mon_host:
            ceph_client = CephTask(ctxt)
            res = ceph_client.ceph_data_balance()
            objects.sysconfig.sys_config_set(
                ctxt, "data_balance", res.get("active"), "bool")
            objects.sysconfig.sys_config_set(
                ctxt, "data_balance_mode", res.get("mode"), "string")
            data_balance = {
                "active": res.get("active"),
                "mode": res.get("mode"),
            }
            return data_balance
        else:
            logger.error("The cluster has no mon role.")
            return "The cluster has no mon role."

    def cluster_data_balance_set(self, ctxt, data_balance):
        action = data_balance.get("action")
        mode = data_balance.get("mode")
        if action not in ['on', 'off']:
            logger.error("Invaild action: %s" % action)
            return "Invaild action: %s" % action
        if mode not in ['crush-compat', 'upmap', None]:
            logger.error("Invaild mode: %s" % mode)
            return "Invaild mode: %s" % mode
        has_mon_host = self.has_monitor_host(ctxt)
        if has_mon_host:
            ceph_client = CephTask(ctxt)
            res = ceph_client.ceph_data_balance(action, mode)
        else:
            logger.error("The cluster has no mon role.")
            return "The cluster has no mon role."
        objects.sysconfig.sys_config_set(
            ctxt, "data_balance", res.get("active"), "bool")
        objects.sysconfig.sys_config_set(
            ctxt, "data_balance_mode", res.get("mode"), "string")
        return res
