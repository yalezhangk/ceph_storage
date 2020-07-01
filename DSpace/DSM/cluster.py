import six
from oslo_log import log as logging

from DSpace import exception as exc
from DSpace import objects
from DSpace.DSM.alert_rule import AlertRuleInitMixin
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.objects.fields import CephVersion
from DSpace.objects.fields import ConfigKey
from DSpace.taskflows.ceph import CephTask
from DSpace.taskflows.cluster import cluster_delete_flow
from DSpace.taskflows.node import NodeTask
from DSpace.taskflows.node import PrometheusTargetMixin
from DSpace.tools.base import SSHExecutor
from DSpace.tools.ceph import CephTool
from DSpace.tools.prometheus import PrometheusTool

logger = logging.getLogger(__name__)


class ClusterHandler(AdminBaseHandler, AlertRuleInitMixin):

    def cluster_get(self, ctxt, cluster_id):
        cluster = objects.Cluster.get_by_id(ctxt, cluster_id)
        return cluster

    def cluster_get_all(self, ctxt, detail=False, marker=None, limit=None,
                        sort_keys=None, sort_dirs=None, filters=None,
                        offset=None):
        clusters = objects.ClusterList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)
        prometheus = PrometheusTool(ctxt)
        # cluster 容量和已配置容量
        logger.debug('get cluster capacity')
        for c in clusters:
            capacity = prometheus.cluster_get_capacity(
                filter={'cluster_id': c.id})
            c.metrics.update({'capacity': capacity})
            if detail:
                ctxt.cluster_id = c.id
                c.capacity = self.cluster_capacity_status_get(ctxt)
        return clusters

    def cluster_get_count(self, ctxt, filters=None):
        return objects.ClusterList.get_count(
            ctxt, filters=filters)

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
        node_task = NodeTask(ctxt, node)
        try:
            PrometheusTargetMixin().target_remove(
                ctxt, node, service='node_exporter')
        except Exception as e:
            logger.error(e)

        try:
            node_task.chrony_uninstall()
            node_task.node_exporter_uninstall()
            node_task.dspace_agent_uninstall()

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

        admin_ips = objects.sysconfig.sys_config_get(ctxt, ConfigKey.ADMIN_IPS)
        admin_ips = admin_ips.split(',')

        success = True
        cluster = clusters[0]
        ctxt.cluster_id = cluster.id
        nodes = objects.NodeList.get_all(
            ctxt, filters={"ip_address": admin_ips,
                           'status': [s_fields.NodeStatus.ACTIVE,
                                      s_fields.NodeStatus.WARNING]}
        )
        if len(nodes) != len(admin_ips):
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
        admin_ips = objects.sysconfig.sys_config_get(ctxt, ConfigKey.ADMIN_IPS)
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
        is_admin = data.get('admin_create')
        cluster = objects.Cluster(
            ctxt, is_admin=is_admin, status=s_fields.ClusterStatus.ACTIVE,
            display_name=data.get('cluster_name'))
        cluster.create()
        admin_begin_action = None
        if not is_admin:
            # add an admin_cluster actions
            admin_cluster = objects.ClusterList.get_all(
                ctxt, filters={'is_admin': True})
            admin_cluster_id = admin_cluster[0].id
            logger.info('admin_cluster_id is:%s', admin_cluster_id)
            ctxt.cluster_id = admin_cluster_id
            admin_begin_action = self.begin_action(
                ctxt, Resource.CLUSTER, Action.CREATE)
        ctxt.cluster_id = cluster.id
        new_cluster_action = self.begin_action(
            ctxt, Resource.CLUSTER, Action.CREATE)
        # TODO check key value
        if ConfigKey.CEPH_VERSION_NAME not in data:
            data[ConfigKey.CEPH_VERSION_NAME] = CephVersion.T2STOR
        for key, value in six.iteritems(data):
            if key == "enable_cephx":
                sysconf = objects.SysConfig(
                    ctxt, key=key, value=value,
                    value_type=s_fields.ConfigType.BOOL)
            else:
                sysconf = objects.SysConfig(
                    ctxt, key=key, value=value,
                    value_type=s_fields.ConfigType.STRING)
            sysconf.create()
        cluster_id = cluster.id
        self.init_alert_rule(ctxt, cluster_id)
        pro_rules = self.get_cluster_prome_rules(cluster_id)
        logger.info('new_cluster_id:%s, pro_rules:%s', cluster, pro_rules)
        self.task_submit(self.update_prometheus_que, pro_rules)
        self.task_submit(self.update_notify_group, cluster_id)
        if not is_admin:
            # add an admin_cluster actions
            self.finish_action(admin_begin_action, cluster.id,
                               cluster.display_name, after_obj=cluster)
        self.finish_action(new_cluster_action, cluster.id,
                           cluster.display_name, after_obj=cluster)
        logger.info('cluster %s init alert_rule task has begin', cluster.id)
        return cluster

    def cluster_update_display_name(self, ctxt, id, name):
        cluster = objects.Cluster.get_by_id(ctxt, id)
        if cluster.display_name != name:
            self._check_cluster_display_name(ctxt, name)
        begin_action = self.begin_action(
                ctxt, resource_type=Resource.CLUSTER,
                action=Action.UPDATE, before_obj=cluster)
        logger.info(
                "update cluster name from %s to %s",
                cluster.display_name, name)
        cluster.display_name = name
        cluster.save()
        self.finish_action(begin_action, resource_id=cluster.id,
                           resource_name=cluster.display_name,
                           after_obj=cluster)
        return cluster

    def _check_cluster_display_name(self, ctxt, display_name):
        filters = {"display_name": display_name}
        clusters = objects.ClusterList.get_all(ctxt, filters=filters)
        if clusters:
            logger.error("cluster display_name duplicate: %s", display_name)
            raise exc.ClusterExists(cluster=display_name)

    def _cluster_delete(self, ctxt, cluster, src_cluster_id, clean_ceph=False,
                        begin_action=None):
        logger.info("trying to delete cluster-%s", cluster.id)
        try:
            pro_rules = self.get_cluster_prome_rules(cluster.id)
            logger.info('will remove pro_rules:%s,cluster_id:%s',
                        pro_rules, cluster.id)
            self.update_prometheus_que(pro_rules, remove=True)
            self.update_notify_group(cluster.id, remove=True)
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
                           cluster, status,
                           err_msg=err_msg)
        ctxt.cluster_id = src_cluster_id
        logger.debug("delete cluster %s finish: %s", cluster.id, msg)
        self.send_websocket(ctxt, cluster, action, msg)

    def _cluster_delete_check(self, ctxt, cluster, clean_ceph):
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
                    s_fields.OsdStatus.DELETING,
                    s_fields.OsdStatus.REPLACE_PREPARING,
                    s_fields.OsdStatus.REPLACE_PREPARED,
                    s_fields.OsdStatus.REPLACING
                ]
            }
        )
        if len(osds):
            raise exc.InvalidInput(_('Cluster has osds in doing task'))
        import_task = objects.sysconfig.sys_config_get(ctxt, "import_task_id")
        if import_task is not None and import_task >= 0:
            raise exc.InvalidInput(_('Cluster is importing'))

        if clean_ceph:
            rgws = objects.RadosgwList.get_count(ctxt)
            rgw_routers = objects.RadosgwRouterList.get_count(ctxt)
            if rgws or rgw_routers:
                raise exc.InvalidInput(_('Please remove radosgw and '
                                         'radosgw routers first'))

    def cluster_delete(self, ctxt, cluster_id, clean_ceph=False):
        logger.debug("delete cluster %s start", cluster_id)
        src_cluster_id = ctxt.cluster_id
        ctxt.cluster_id = cluster_id
        cluster = objects.Cluster.get_by_id(ctxt, cluster_id)
        self._cluster_delete_check(ctxt, cluster, clean_ceph)
        ctxt.cluster_id = src_cluster_id
        begin_action = self.begin_action(
            ctxt, Resource.CLUSTER, Action.DELETE, before_obj=cluster)
        ctxt.cluster_id = cluster_id
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
        if not objects.NodeList.get_count(ctxt):
            return {}
        return objects.ServiceList.service_status_get(ctxt, names=names)

    def cluster_host_status_get(self, ctxt):
        query_all = objects.NodeList.get_status(ctxt)
        status = {s_fields.NodeStatus.ACTIVE: 0,
                  s_fields.NodeStatus.ERROR: 0,
                  s_fields.NodeStatus.WARNING: 0,
                  "progress": 0}
        for [k, v] in query_all:
            if k in [s_fields.NodeStatus.ACTIVE,
                     s_fields.NodeStatus.ERROR,
                     s_fields.NodeStatus.WARNING]:
                status[k] = v
            else:
                status["progress"] += v
        return status

    def cluster_pool_status_get(self, ctxt):
        logger.info("try get pool status")
        query_all = objects.PoolList.get_status(ctxt)
        num = 0
        status = {s_fields.PoolStatus.ACTIVE: 0,
                  s_fields.PoolStatus.DEGRADED: 0,
                  s_fields.PoolStatus.RECOVERING: 0,
                  s_fields.PoolStatus.PROCESSING: 0,
                  s_fields.PoolStatus.WARNING: 0,
                  s_fields.PoolStatus.ERROR: 0}
        for [k, v] in query_all:
            if k in [s_fields.PoolStatus.ACTIVE,
                     s_fields.PoolStatus.DEGRADED,
                     s_fields.PoolStatus.RECOVERING,
                     s_fields.PoolStatus.WARNING,
                     s_fields.PoolStatus.ERROR]:
                status[k] = v
            elif k in [s_fields.PoolStatus.PROCESSING,
                       s_fields.PoolStatus.CREATING,
                       s_fields.PoolStatus.DELETING]:
                num += v
        status[s_fields.PoolStatus.PROCESSING] = num
        logger.info("pool status: %s", status)
        return status

    def cluster_osd_status_get(self, ctxt):
        query_all = objects.OsdList.get_status(ctxt)
        status = {s_fields.OsdStatus.ACTIVE: 0,
                  s_fields.OsdStatus.PROCESSING: 0,
                  s_fields.OsdStatus.WARNING: 0,
                  s_fields.OsdStatus.OFFLINE: 0,
                  s_fields.OsdStatus.ERROR: 0}
        for [k, v] in query_all:
            if k == s_fields.OsdStatus.ACTIVE:
                status[s_fields.OsdStatus.ACTIVE] = v
            elif k == s_fields.OsdStatus.WARNING:
                status[s_fields.OsdStatus.WARNING] = v
            elif k == s_fields.OsdStatus.OFFLINE:
                status[s_fields.OsdStatus.OFFLINE] = v
            elif k == s_fields.OsdStatus.ERROR:
                status[s_fields.OsdStatus.ERROR] = v
            else:
                status[s_fields.OsdStatus.PROCESSING] = v
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
        data_balance = {
            "active": False,
            "mode": "none"
        }
        if has_mon_host:
            ceph_client = CephTask(ctxt)
            if not ceph_client.is_module_enable("balancer"):
                balance_enable = False
                balance_mode = "none"
            else:
                res = ceph_client.balancer_status()
                balance_enable = res.get('active')
                balance_mode = res.get('mode')
            objects.sysconfig.sys_config_set(
                ctxt, "data_balance", balance_enable, "bool")
            objects.sysconfig.sys_config_set(
                ctxt, "data_balance_mode", balance_mode, "string")
            data_balance["active"] = balance_enable
            data_balance["mode"] = balance_mode
        else:
            logger.error("cluster has no mon role or can't connect to mon.")
        return data_balance

    def _data_balancer_set(self, ctxt, action_log, action, mode):

        data_balance = {
            "active": False,
            "mode": "none"
        }

        if action not in ['on', 'off']:
            logger.error("Invaild action: %s" % action)
            raise exc.InvalidInput(_('Invaild action: %s') % action)
        if mode not in ['crush-compat', 'upmap', None]:
            logger.error("Invaild mode: %s" % mode)
            raise exc.InvalidInput(_('Invaild mode: %s') % action)
        has_mon_host = self.has_monitor_host(ctxt)
        if has_mon_host:
            ceph_client = CephTask(ctxt)
            res = ceph_client.ceph_data_balance(action, mode)
            data_balance["active"] = res.get("active")
            data_balance["mode"] = res.get("mode")
        else:
            logger.error("cluster has no mon role or can't connect to mon.")
        objects.sysconfig.sys_config_set(
            ctxt, "data_balance", data_balance.get("active"), "bool")
        objects.sysconfig.sys_config_set(
            ctxt, "data_balance_mode", data_balance.get("mode"), "string")

    def cluster_data_balance_set(self, ctxt, data_balance):
        self.check_mon_host(ctxt)
        action = data_balance.get("action")
        mode = data_balance.get("mode")
        action_map = {
            'on': Action.DATA_BALANCE_ON,
            'off': Action.DATA_BALANCE_OFF
        }
        action_log = self.begin_action(
            ctxt, Resource.CLUSTER, action_map[action])
        try:
            self._data_balancer_set(ctxt, action_log, action, mode)
            log_err = None
            log_status = 'success'
        except Exception as e:
            logger.exception(e)
            log_status = 'failed'
            if action == 'on':
                log_err = _("Cluster balance enable failed: %s") % str(e)
            else:
                log_err = _("Cluster balance disable failed: %s") % str(e)
        cluster_id = ctxt.cluster_id
        cluster = objects.Cluster.get_by_id(ctxt, cluster_id)
        self.finish_action(action_log, cluster_id, cluster.display_name,
                           after_obj=data_balance, status=log_status,
                           err_msg=log_err)
        if action == 'on':
            msg = _("Cluster balance enabled")
            ws_action = "CLUSTER_BALANCE_ENABLE"
        else:
            msg = _("Cluster balance disable")
            ws_action = "CLUSTER_BALANCE_DISABLE"
        self.send_websocket(ctxt, cluster, ws_action, msg)
        return data_balance

    def cluster_pause(self, ctxt, enable=True):
        self.check_mon_host(ctxt)
        if enable:
            action = Action.Cluster_PAUSE
        else:
            action = Action.Cluster_UNPAUSE
        begin_action = self.begin_action(ctxt, Resource.CLUSTER, action)
        try:
            cluster = objects.Cluster.get_by_id(ctxt, ctxt.cluster_id)
            ceph_client = CephTask(ctxt)
            ceph_client.cluster_pause(enable)
            if enable:
                msg = _("Cluster Pause Success")
            else:
                msg = _("Cluster Unpause Success")
            action = "CLUSTER_PAUSE"
            status = 'success'
            err_msg = None
        except Exception as e:
            logger.exception('get cluster info error: %s', e)
            if enable:
                msg = _("Cluster Pause Error")
            else:
                msg = _("Cluster Unpause Error")
            action = "CLUSTER_PAUSE_ERROR"
            status = 'fail'
            err_msg = str(e)
        self.finish_action(begin_action, ctxt.cluster_id, cluster.display_name,
                           after_obj=cluster, status=status, err_msg=err_msg)
        self.send_websocket(ctxt, cluster, action, msg)

    def cluster_status(self, ctxt):
        logger.info("get cluster status")
        has_mon_host = self.monitor_count(ctxt)
        if not has_mon_host:
            res = {
                "created": False,
                "status": False,
                "pause": False,
                "balancer": False
            }
        elif not self.has_monitor_host(ctxt):
            res = {
                "created": True,
                "status": False,
                "pause": False,
                "balancer": False
            }
        else:
            ceph_client = CephTask(ctxt)
            res = ceph_client.cluster_status()
            res['status'] = True
            res["created"] = True
        logger.info("cluster status: %s", res)
        return res

    def cluster_capacity_get(self, ctxt, pool_id):
        # pool_id -> Pool object id
        prometheus = PrometheusTool(ctxt)
        if pool_id:
            # pool 容量和已配置容量
            logger.debug('begin get pool_id:%s capacity', pool_id)
            pool = objects.Pool.get_by_id(ctxt, int(pool_id))
            result = prometheus.pool_get_provisioned_capacity(
                ctxt, pool.pool_id)
            logger.info('get pool_id:%s capacity success, data:%s',
                        pool_id, result)
            return result
        else:
            # cluster 容量和已配置容量
            logger.debug('get cluster capacity')
            cluster_capacity = prometheus.cluster_get_capacity()
            logger.info('get cluster capacity success, data:%s',
                        cluster_capacity)
        return cluster_capacity
