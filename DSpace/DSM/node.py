import six
from netaddr import IPAddress
from netaddr import IPNetwork
from oslo_log import log as logging

from DSpace import exception as exc
from DSpace import objects
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.taskflows.include import include_clean_flow
from DSpace.taskflows.include import include_flow
from DSpace.taskflows.node import NodeMixin
from DSpace.taskflows.node import NodeTask
from DSpace.taskflows.probe import ProbeTask
from DSpace.tools.prometheus import PrometheusTool
from DSpace.utils import cluster_config as ClusterConfg
from DSpace.utils import logical_xor
from DSpace.utils import validator

logger = logging.getLogger(__name__)


class NodeHandler(AdminBaseHandler):

    def node_get(self, ctxt, node_id, expected_attrs=None):
        node_info = objects.Node.get_by_id(
            ctxt, node_id, expected_attrs=expected_attrs)
        return node_info

    def node_update(self, ctxt, node_id, data):
        node = objects.Node.get_by_id(ctxt, node_id)
        for k, v in six.iteritems(data):
            setattr(node, k, v)
        node.save()
        return node

    def _remove_node_resource(self, ctxt, node):
        rpc_services = objects.RPCServiceList.get_all(
            ctxt,
            filters={
                "service_name": "agent",
                "node_id": node.id
            }
        )
        for rpc_service in rpc_services:
            rpc_service.destroy()

        disks = objects.DiskList.get_all(ctxt, filters={"node_id": node.id})
        for disk in disks:
            disk.destroy()

        networks = objects.NetworkList.get_all(
            ctxt, filters={"node_id": node.id})
        for network in networks:
            network.destroy()

    def _node_delete(self, ctxt, node, begin_action):
        try:
            if node.role_monitor:
                self._mon_uninstall(ctxt, node)
            if node.role_storage:
                self._storage_uninstall(ctxt, node)
            if node.role_block_gateway:
                self._bgw_uninstall(ctxt, node)
            if node.role_object_gateway:
                self._rgw_uninstall(ctxt, node)
            node_task = NodeTask(ctxt, node)
            node_task.ceph_package_uninstall()
            node_task.chrony_uninstall()
            node_task.node_exporter_uninstall()
            node_task.dspace_agent_uninstall()
            node_task.prometheus_target_config(action='remove',
                                               service='node_exporter')
            self._remove_node_resource(ctxt, node)

            node.destroy()
            logger.info("node delete success")
            msg = _("node remove success: {}").format(node.hostname)
            status = 'success'
            err_msg = None
            op_status = "DELETE_SUCCESS"
        except Exception as e:
            status = s_fields.NodeStatus.ERROR
            node.status = status
            node.save()
            logger.exception("node delete error: %s", e)
            msg = _("node remove error: {}").format(node.hostname)
            err_msg = str(e)
            op_status = "DELETE_ERROR"
        self.finish_action(begin_action, node.id, node.hostname,
                           objects.json_encode(node), status, err_msg=err_msg)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, node, op_status, msg)

    def node_update_rack(self, ctxt, node_id, rack_id):
        node = objects.Node.get_by_id(ctxt, node_id, expected_attrs=['osds'])
        osd_crush_rule_ids = set([i.crush_rule_id for i in node.osds])
        pools = objects.PoolList.get_all(
            ctxt, filters={"crush_rule_id": osd_crush_rule_ids})
        logger.info("node_update_rack, osd_crush_rule_ids: %s",
                    osd_crush_rule_ids)
        if pools:
            logger.error("node %s has osds already in pool %s, can't move",
                         node.hostname, pools[0].display_name)
            raise exc.NodeMoveNotAllow(node=node.hostname,
                                       pool=pools[0].display_name)
        begin_action = self.begin_action(ctxt, Resource.NODE,
                                         Action.NODE_UPDATE_RACK)
        node.rack_id = rack_id
        node.save()
        self.finish_action(begin_action, node_id, node.hostname,
                           objects.json_encode(node))
        return node

    def _node_delete_check(self, ctxt, node):
        allowed_status = [s_fields.NodeStatus.ACTIVE,
                          s_fields.NodeStatus.ERROR]
        if node.status not in allowed_status:
            raise exc.InvalidInput(_("Node status not allow!"))
        if node.role_admin:
            raise exc.InvalidInput(_("admin role could not delete!"))
        if node.role_monitor:
            self._mon_uninstall_check(ctxt, node)
        if node.role_storage:
            self._storage_uninstall_check(ctxt, node)
        disk_partition_num = objects.DiskPartitionList.get_count(
            ctxt, filters={"node_id": node.id, "role": [
                s_fields.DiskPartitionRole.CACHE,
                s_fields.DiskPartitionRole.DB,
                s_fields.DiskPartitionRole.WAL,
                s_fields.DiskPartitionRole.JOURNAL,
            ]}
        )
        if disk_partition_num:
            raise exc.InvalidInput(_("Please remove disk partition first!"))

    def node_delete(self, ctxt, node_id):
        node = objects.Node.get_by_id(ctxt, node_id)
        # judge node could be delete
        self._node_delete_check(ctxt, node)
        begin_action = self.begin_action(ctxt, Resource.NODE, Action.DELETE)
        node.status = s_fields.NodeStatus.DELETING
        node.save()
        self.task_submit(self._node_delete, ctxt, node, begin_action)
        return node

    def _node_get_metrics_overall(self, ctxt, nodes):
        # TODO get all data at once
        prometheus = PrometheusTool(ctxt)
        for node in nodes:
            prometheus.node_get_metrics_overall(node)

    def node_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                     sort_dirs=None, filters=None, offset=None,
                     expected_attrs=None):
        nodes = objects.NodeList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)
        self._node_get_metrics_overall(ctxt, nodes)
        return nodes

    def node_get_count(self, ctxt, filters=None):
        return objects.NodeList.get_count(ctxt, filters=filters)

    def _set_ceph_conf(self, ctxt, key, value, value_type, group="global",
                       display_description=None):
        filters = {
            "group": group,
            "key": key
        }
        cephconf = objects.CephConfigList.get_all(ctxt, filters=filters)
        if not cephconf:
            cephconf = objects.CephConfig(
                ctxt, group=group, key=key,
                value=value,
                value_type=value_type,
                display_description=display_description,
                cluster_id=ctxt.cluster_id
            )
            cephconf.create()
        else:
            cephconf = cephconf[0]
            cephconf.value = value
            cephconf.save()
        return cephconf

    def _mon_install_check(self, ctxt, node=None):
        if node and node.role_monitor:
            raise exc.InvalidInput(_("The monitor role has been installed."))
        public_network = objects.sysconfig.sys_config_get(
            ctxt, key="public_cidr"
        )
        cluster_network = objects.sysconfig.sys_config_get(
            ctxt, key="cluster_cidr"
        )
        max_mon_num = objects.sysconfig.sys_config_get(
            ctxt, key="max_monitor_num"
        )
        mon_num = objects.NodeList.get_count(
            ctxt, filters={"role_monitor": True}
        )
        if not public_network or not cluster_network:
            raise exc.InvalidInput(_("Please set network planning"))
        if mon_num >= max_mon_num:
            raise exc.InvalidInput(_("Max monitor num is %s" % max_mon_num))

    def _mon_uninstall_check(self, ctxt, node):
        if not node.role_monitor:
            raise exc.InvalidInput(_(
                "The monitor role has not yet been installed"))
        osd_num = objects.OsdList.get_count(ctxt)
        mon_num = objects.NodeList.get_count(
            ctxt, filters={"role_monitor": True}
        )
        if osd_num and mon_num == 1:
            raise exc.InvalidInput(_("Please remove osd first!"))

    def _storage_install_check(self, ctxt, node):
        if node.role_storage:
            raise exc.InvalidInput(_("The storage role has been installed."))

    def _storage_uninstall_check(self, ctxt, node):
        if not node.role_storage:
            raise exc.InvalidInput(_(
                "The storage role has not yet been installed"))
        node_osds = objects.OsdList.get_count(
            ctxt, filters={"node_id": node.id}
        )
        if node_osds:
            raise exc.InvalidInput(_("Node %s has osd!" % node.hostname))

    def _mds_install_check(self, ctxt, node):
        if node.role_admin:
            raise exc.InvalidInput(_("The admin role has been installed."))

    def _mds_uninstall_check(self, ctxt, node):
        if not node.role_admin:
            raise exc.InvalidInput(_(
                "The admin role has not yet been installed"))

    def _rgw_install_check(self, ctxt, node):
        if node.role_object_gateway:
            raise exc.InvalidInput(_(
                "The object gateway role has been installed."))

    def _rgw_uninstall_check(self, ctxt, node):
        if not node.role_object_gateway:
            raise exc.InvalidInput(_(
                "The object gateway role has not yet been installed"))

    def _bgw_install_check(self, ctxt, node):
        if node.role_block_gateway:
            raise exc.InvalidInput(_(
                "The block gateway role has been installed."))

    def _bgw_uninstall_check(self, ctxt, node):
        if not node.role_block_gateway:
            raise exc.InvalidInput(_(
                "The block gateway role has not yet been installed"))

    def _mon_install(self, ctxt, node):
        node.status = s_fields.NodeStatus.DEPLOYING_ROLE
        node.role_monitor = True
        node.save()

        mon_nodes = objects.NodeList.get_all(
            ctxt, filters={"role_monitor": True}
        )
        if len(mon_nodes) == 1:
            # init ceph config
            public_network = objects.sysconfig.sys_config_get(
                ctxt, key="public_cidr"
            )
            cluster_network = objects.sysconfig.sys_config_get(
                ctxt, key="cluster_cidr"
            )
            mon_host = str(node.public_ip)
            mon_initial_members = node.hostname
            new_cluster_config = {
                'fsid': {'type': 'string', 'value': ctxt.cluster_id},
                'mon_host': {'type': 'string',
                             'value': mon_host},
                'osd_objectstore': {'type': 'string', 'value': 'bluestore'},
                'mon_pg_warn_min_per_osd': {'type': 'int', 'value': 0},
                'debug_mon': {'type': 'int', 'value': 10},
                'mon_initial_members': {'type': 'string',
                                        'value': mon_initial_members},
                'public_network': {'type': 'string', 'value': public_network},
                'cluster_network': {'type': 'string', 'value': cluster_network}
            }
            configs = {}
            configs.update(ClusterConfg.default_cluster_configs)
            configs.update(new_cluster_config)
            for key, value in configs.items():
                self._set_ceph_conf(ctxt,
                                    key=key,
                                    value=value.get('value'),
                                    value_type=value.get('type'))
        else:
            mon_host = ",".join(
                [str(n.public_ip) for n in mon_nodes]
            )
            mon_initial_members = ",".join([n.hostname for n in mon_nodes])
            self._set_ceph_conf(ctxt,
                                key="mon_host",
                                value=mon_host,
                                value_type='string')
            self._set_ceph_conf(ctxt,
                                key="mon_initial_members",
                                value=mon_initial_members,
                                value_type='string')
        node_task = NodeTask(ctxt, node)
        node_task.ceph_mon_install()
        # sync config file
        osd_nodes = objects.NodeList.get_all(
            ctxt, filters={"role_storage": True}
        )
        configs = [
            {"group": "global", "key": "mon_host", "value": mon_host},
            {"group": "global", "key": "mon_initial_members",
             "value": mon_initial_members}
        ]
        nodes = mon_nodes + osd_nodes
        for n in nodes:
            task = NodeTask(ctxt, n)
            for config in configs:
                task.ceph_config_update(ctxt, config)
        node_task.prometheus_target_config(action='add', service='mgr')
        return node

    def _mon_uninstall(self, ctxt, node):
        node.status = s_fields.NodeStatus.REMOVING_ROLE
        node.role_monitor = False
        node.save()

        mon_nodes = objects.NodeList.get_all(
            ctxt, filters={"role_monitor": True}
        )
        task = NodeTask(ctxt, node)
        if len(mon_nodes):
            # update ceph config
            mon_host = ",".join(
                [str(n.public_ip) for n in mon_nodes]
            )
            mon_initial_members = ",".join([n.hostname for n in mon_nodes])
            self._set_ceph_conf(ctxt,
                                key="mon_host",
                                value=mon_host,
                                value_type='string')
            self._set_ceph_conf(ctxt,
                                key="mon_initial_members",
                                value=mon_initial_members,
                                value_type='string')
            task.ceph_mon_uninstall(last_mon=False)
            osd_nodes = objects.NodeList.get_all(
                ctxt, filters={"role_storage": True}
            )
            configs = [
                {"group": "global", "key": "mon_host", "value": mon_host},
                {"group": "global", "key": "mon_initial_members",
                 "value": mon_initial_members}
            ]
            nodes = mon_nodes + osd_nodes
            for n in nodes:
                task = NodeTask(ctxt, n)
                for config in configs:
                    task.ceph_config_update(ctxt, config)
        else:
            task.ceph_mon_uninstall(last_mon=True)
        task.prometheus_target_config(action='remove', service='mgr')
        return node

    def _storage_install(self, ctxt, node):
        node.status = s_fields.NodeStatus.DEPLOYING_ROLE
        node_task = NodeTask(ctxt, node)
        node_task.ceph_osd_package_install()

        node.role_storage = True
        node.save()
        return node

    def _storage_uninstall(self, ctxt, node):
        node.status = s_fields.NodeStatus.REMOVING_ROLE
        node_task = NodeTask(ctxt, node)
        node_task.ceph_osd_package_uninstall()

        node.role_storage = False
        node.save()
        return node

    def _mds_install(self, ctxt, node):
        pass

    def _mds_uninstall(self, ctxt, node):
        pass

    def _rgw_install(self, ctxt, node):
        node.status = s_fields.NodeStatus.DEPLOYING_ROLE
        node.role_object_gateway = True
        node.save()
        return node

    def _rgw_uninstall(self, ctxt, node):
        node.status = s_fields.NodeStatus.REMOVING_ROLE
        node.role_object_gateway = False
        node.save()
        return node

    def _bgw_install(self, ctxt, node):
        node.status = s_fields.NodeStatus.DEPLOYING_ROLE
        node.role_block_gateway = True
        node.save()
        return node

    def _bgw_uninstall(self, ctxt, node):
        node.status = s_fields.NodeStatus.REMOVING_ROLE
        node.role_block_gateway = False
        node.save()
        return node

    def _node_roles_set(self, ctxt, node, i_roles, u_roles, begin_action):
        install_role_map = {
            'monitor': self._mon_install,
            'storage': self._storage_install,
            'mds': self._mds_install,
            'radosgw': self._rgw_install,
            'blockgw': self._bgw_install,
        }
        uninstall_role_map = {
            'monitor': self._mon_uninstall,
            'storage': self._storage_uninstall,
            'mds': self._mds_uninstall,
            'radosgw': self._rgw_uninstall,
            'blockgw': self._bgw_uninstall,
        }
        try:
            for role in i_roles:
                func = install_role_map.get(role)
                func(ctxt, node)
            for role in u_roles:
                func = uninstall_role_map.get(role)
                func(ctxt, node)
            status = s_fields.NodeStatus.ACTIVE
            logger.info('set node roles success')
            msg = _("set roles success: {}").format(node.hostname)
            err_msg = None
            op_status = "SET_ROLES_SUCCESS"
        except Exception as e:
            status = s_fields.NodeStatus.ERROR
            logger.exception('set node roles failed %s', e)
            msg = _("set roles error {}".format(str(e)))
            err_msg = str(e)
            op_status = "SET_ROLES_ERROR"
        node.status = status
        node.save()
        # send ws message
        self.finish_action(begin_action, node.id, node.hostname,
                           objects.json_encode(node), status,
                           err_msg=err_msg)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, node, op_status, msg)

    def node_roles_set(self, ctxt, node_id, data):
        node = objects.Node.get_by_id(ctxt, node_id)
        if node.status != s_fields.NodeStatus.ACTIVE:
            raise exc.InvalidInput(_("Only host's status is active can set"
                                     "role(host_name: %s)" % node.hostname))

        i_roles = set(data.get('install_roles'))
        u_roles = set(data.get('uninstall_roles'))
        if not len(i_roles) and not len(u_roles):
            raise exc.InvalidInput(_("Please provide roles"))
        if len(list(i_roles.intersection(u_roles))):
            raise exc.InvalidInput(_("Can't not set and unset the same role"))
        install_check_role_map = {
            'monitor': self._mon_install_check,
            'storage': self._storage_install_check,
            'mds': self._mds_install_check,
            'radosgw': self._rgw_install_check,
            'blockgw': self._bgw_install_check
        }
        uninstall_check_role_map = {
            'monitor': self._mon_uninstall_check,
            'storage': self._storage_uninstall_check,
            'mds': self._mds_uninstall_check,
            'radosgw': self._rgw_uninstall_check,
            'blockgw': self._bgw_uninstall_check
        }
        for role in i_roles:
            func = install_check_role_map.get(role)
            func(ctxt, node)
        for role in u_roles:
            func = uninstall_check_role_map.get(role)
            func(ctxt, node)
        deploying_nodes = objects.NodeList.get_all(
            ctxt,
            filters={
                "status": [s_fields.NodeStatus.DEPLOYING_ROLE,
                           s_fields.NodeStatus.REMOVING_ROLE]
            }
        )
        if deploying_nodes:
            raise exc.InvalidInput(_("Only one node can set roles at the"
                                     " same time"))
        if len(i_roles):
            node.status = s_fields.NodeStatus.DEPLOYING_ROLE
        else:
            node.status = s_fields.NodeStatus.REMOVING_ROLE
        begin_action = self.begin_action(ctxt, Resource.NODE, Action.SET_ROLES)
        logger.info('node %s roles check pass', node.hostname)
        self.task_submit(self._node_roles_set, ctxt, node, i_roles, u_roles,
                         begin_action)
        return node

    def _node_create(self, ctxt, node, data):
        begin_action = self.begin_action(ctxt, Resource.NODE, Action.CREATE)
        try:
            node_task = NodeTask(ctxt, node)
            node_task.dspace_agent_install()
            node_task.chrony_install()
            node_task.node_exporter_install()
            roles = data.get('roles', "").split(',')
            role_monitor = "monitor" in roles
            role_storage = "storage" in roles
            role_admin = "admin" in roles
            role_block_gateway = "blockgw" in roles
            role_object_gateway = "objectgw" in roles
            if role_admin:
                node.role_admin = True
            if role_monitor:
                self._mon_install(ctxt, node)
            if role_storage:
                self._storage_install(ctxt, node)
            if role_block_gateway:
                self._bgw_install(ctxt, node)
            if role_object_gateway:
                self._rgw_install(ctxt, node)
            status = s_fields.NodeStatus.ACTIVE
            logger.info('create node success, node ip: {}'.format(
                        node.ip_address))
            msg = _("node create success")
            op_status = "CREATE_SUCCESS"
            err_msg = None
        except Exception as e:
            status = s_fields.NodeStatus.ERROR
            logger.exception('create node error, node ip: %s, reason: %s',
                             node.ip_address, e)
            msg = _("node create error, reason: {}".format(str(e)))
            err_msg = str(e)
            op_status = "CREATE_ERROR"
        node.status = status
        node.save()

        node_task.prometheus_target_config(action='add',
                                           service='node_exporter')
        self.finish_action(begin_action, node.id, node.hostname,
                           objects.json_encode(node), status,
                           err_msg=err_msg)
        # send ws message
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, node, op_status, msg)
        return node

    def node_create(self, ctxt, data):
        logger.debug("add node to cluster {}".format(data.get('ip_address')))

        NodeMixin._check_node_ip_address(ctxt, data)
        roles = data.get('roles', "").split(',')
        role_monitor = "monitor" in roles
        if role_monitor:
            self._mon_install_check(ctxt)

        node = objects.Node(
            ctxt, ip_address=data.get('ip_address'),
            hostname=data.get('hostname'),
            password=data.get('password'),
            cluster_ip=data.get('cluster_ip'),
            public_ip=data.get('public_ip'),
            status=s_fields.NodeStatus.CREATING)
        node.create()
        self.task_submit(self._node_create, ctxt, node, data)
        return node

    def node_get_infos(self, ctxt, data):
        logger.debug("get node infos: {}".format(data.get('ip_address')))

        node = objects.Node(
            ctxt, ip_address=data.get('ip_address'),
            password=data.get('password'))

        node_task = NodeTask(ctxt, node)
        node_infos = node_task.node_get_infos()
        node_infos['admin_ip'] = data.get('ip_address')
        return node_infos

    def node_check(self, ctxt, data):
        check_items = ["hostname", "selinux", "ceph_ports", "ceph_package",
                       "network", "athena_ports", "firewall", "container"]
        res = self._node_check(ctxt, data, check_items)
        return res

    def _node_check_ip(self, ctxt, data):
        admin_cidr = objects.sysconfig.sys_config_get(
            ctxt, key="admin_cidr")
        public_cidr = objects.sysconfig.sys_config_get(
            ctxt, key="public_cidr")
        cluster_cidr = objects.sysconfig.sys_config_get(
            ctxt, key="cluster_cidr")
        admin_ip = data.get('ip_address')
        public_ip = data.get('public_ip')
        cluster_ip = data.get('cluster_ip')
        # check ip
        node = self._node_get_by_ip(ctxt, "ip_address", admin_ip, "*")
        if node:
            raise exc.Duplicate(_("Admin ip exists"))
        if IPAddress(admin_ip) not in IPNetwork(admin_cidr):
            raise exc.InvalidInput("admin ip not in admin cidr ({})"
                                   "".format(admin_cidr))
        # cluster_ip
        node = self._node_get_by_ip(ctxt, "cluster_ip", cluster_ip,
                                    ctxt.cluster_id)
        if node:
            raise exc.Duplicate(_("Cluster ip exists"))
        if (IPAddress(cluster_ip) not in
                IPNetwork(cluster_cidr)):
            raise exc.InvalidInput("cluster ip not in cluster cidr ({})"
                                   "".format(cluster_cidr))
        # public_ip
        node = self._node_get_by_ip(ctxt, "public_ip", public_ip,
                                    ctxt.cluster_id)
        if node:
            raise exc.Duplicate(_("Public ip exists"))
        if (IPAddress(public_ip) not in
                IPNetwork(public_cidr)):
            raise exc.InvalidInput("public ip not in public cidr ({})"
                                   "".format(public_cidr))

    def _nodes_inclusion_check(self, ctxt, datas):
        include_tag = objects.sysconfig.sys_config_get(ctxt, 'is_import')
        if include_tag:
            raise exc.InvalidInput(_("Please Clean First"))
        for data in datas:
            self._node_check_ip(ctxt, data)

    def _nodes_inclusion(self, ctxt, t, datas, begin_action=None):
        try:
            include_flow(ctxt, t, datas)
            status = 'success'
            err_msg = None
        except Exception as e:
            status = 'fail'
            err_msg = str(e)
        cluster_id = ctxt.cluster_id
        cluster = objects.Cluster.get_by_id(ctxt, cluster_id)
        cluster_name = cluster.display_name
        self.finish_action(begin_action, cluster_id, cluster_name,
                           objects.json_encode(cluster), status,
                           err_msg=err_msg)

    def nodes_inclusion(self, ctxt, datas):
        logger.debug("include nodes: {}", datas)
        # check
        self._nodes_inclusion_check(ctxt, datas)

        logger.debug("include nodes check pass: {}", datas)
        begin_action = self.begin_action(ctxt, Resource.CLUSTER,
                                         Action.CLUSTER_INCLUDE)
        t = objects.Task(
            ctxt,
            name="Import Cluster",
            description="Import Cluster",
            current="",
            step_num=0,
            status=s_fields.TaskStatus.RUNNING,
            step=0
        )
        t.create()
        self.task_submit(self._nodes_inclusion, ctxt, t, datas, begin_action)
        return t

    def _nodes_inclusion_clean_check(self, ctxt):
        include_tag = objects.sysconfig.sys_config_get(ctxt, 'is_import')
        if not include_tag:
            raise exc.InvalidInput(_("Please Import First"))

    def _nodes_inclusion_clean(self, ctxt, t, begin_action=None):
        try:
            include_clean_flow(ctxt, t)
            status = 'success'
            err_msg = None
        except Exception as e:
            status = 'fail'
            err_msg = str(e)
        cluster_id = ctxt.cluster_id
        cluster = objects.Cluster.get_by_id(ctxt, cluster_id)
        cluster_name = cluster.display_name
        self.finish_action(begin_action, cluster_id, cluster_name,
                           objects.json_encode(cluster), status,
                           err_msg=err_msg)

    def nodes_inclusion_clean(self, ctxt):
        logger.debug("include delete nodes")
        self._nodes_inclusion_clean_check(ctxt)
        begin_action = self.begin_action(ctxt, Resource.CLUSTER,
                                         Action.CLUSTER_INCLUDE)
        t = objects.Task(
            ctxt,
            name="Clean Import Cluster",
            description="Clean Import Cluster",
            current="",
            step_num=0,
            status=s_fields.TaskStatus.RUNNING,
            step=0
        )
        t.create()
        self.task_submit(
            self._nodes_inclusion_clean, ctxt, t, begin_action)
        return t

    def _nodes_inclusion_check_admin_ips(self, ctxt, datas):
        """Check admin_ips in nodes

        :return list: The admin_ip not in datas
        """
        cluster = objects.Cluster.get_by_id(ctxt, ctxt.cluster_id)
        if not cluster.is_admin:
            return []
        admin_ips = objects.sysconfig.sys_config_get(ctxt, "admin_ips")
        if admin_ips:
            admin_ips = admin_ips.split(',')
        for data in datas:
            admin_ip = data.get("admin_ip")
            if admin_ip in admin_ips:
                admin_ips.pop(admin_ip)
        return admin_ips

    def _nodes_inclusion_check_all_ips(self, ctxt, datas):
        node_ips = [data.get('ip_address') for data in datas]
        data = datas[0]
        node = objects.Node(ip_address=data.get('ip_address'),
                            password=data.get('password'))
        node_task = ProbeTask(ctxt, node)
        infos = node_task.probe_cluster_nodes()
        leaks = []
        for n in infos['nodes']:
            if n not in node_ips:
                leaks.append(n)
        extras = []
        for n in node_ips:
            if n not in infos['nodes']:
                extras.append(n)
        return leaks, extras

    def _node_check_port(self, node_task, ports):
        res = []
        for po in ports:
            if not node_task.check_port(po):
                res.append({"port": po, "status": False})
            else:
                res.append({"port": po, "status": True})
        return res

    def _node_get_by_ip(self, ctxt, key, ip, cluster_id):
        nodes = objects.NodeList.get_all(
            ctxt,
            filters={
                key: ip,
                "cluster_id": cluster_id
            }
        )
        if nodes:
            return nodes[0]
        else:
            return None

    def _node_check_ips(self, ctxt, data):
        res = {}
        admin_ip = data.get('ip_address')
        public_ip = data.get('public_ip')
        cluster_ip = data.get('cluster_ip')
        li_ip = [admin_ip, public_ip, cluster_ip]
        if not all(li_ip):
            raise exc.Invalid(_('admin_ip,cluster_ip,public_ip is required'))
        # format
        for ip in li_ip:
            validator.validate_ip(ip)
        # admin_ip
        node = self._node_get_by_ip(
            ctxt, "ip_address", admin_ip, "*")
        res['check_admin_ip'] = False if node else True
        # cluster_ip
        node = self._node_get_by_ip(
            ctxt, "cluster_ip", cluster_ip, ctxt.cluster_id)
        res['check_cluster_ip'] = False if node else True
        # public_ip
        node = self._node_get_by_ip(
            ctxt, "public_ip", public_ip, ctxt.cluster_id)
        res['check_public_ip'] = False if node else True
        # TODO delete it
        res['check_gateway_ip'] = True
        return res

    def _node_check_roles(self, roles, services):
        res = {}
        # add default value
        for s in roles:
            res[s] = True

        # update value
        if logical_xor("ceph-osd" in services, "storage" in roles):
            res['storage'] = False
        if logical_xor("ceph-mon" in services, "monitor" in roles):
            res['monitor'] = False

        data = []
        for k, v in six.iteritems(res):
            data.append({
                'role': k,
                'status': v
            })
        return data

    def _node_check(self, ctxt, data, items=None):
        if not items:
            logger.error("items empty!")
            raise exc.ProgrammingError("items empty!")
        res = {}

        # check input info
        res.update(self._node_check_ips(ctxt, data))

        # connection
        node = objects.Node(
            ctxt, ip_address=data.get('ip_address'),
            password=data.get('password'))
        node_task = NodeTask(ctxt, node)
        node_infos = node_task.node_get_infos()

        # check connection
        hostname = node_infos.get('hostname')
        res['check_through'] = True if hostname else False
        if not hostname:
            return res
        if "hostname" in items:
            if objects.NodeList.get_all(ctxt, filters={"hostname": hostname}):
                res['check_hostname'] = False
            else:
                res['check_hostname'] = True
        # check ceph package
        if "ceph_package" in items:
            r = node_task.check_ceph_is_installed()
            if r:
                res['check_Installation_package'] = False
            else:
                res['check_Installation_package'] = True

        # check selinux
        if "selinux" in items:
            res['check_SELinux'] = node_task.check_selinux()
        # check port
        if "ceph_ports" in items:
            # TODO: move to db
            ports = [6789, 9876, 9100, 9283, 7480]
            res['check_ceph_port'] = self._node_check_port(node_task, ports)
        if "athena_ports" in items:
            # TODO: move to db
            ports = [9100, 2083]
            res['check_athena_port'] = self._node_check_port(node_task, ports)
        if "network" in items:
            public_ip = data.get('public_ip')
            cluster_ip = data.get('cluster_ip')
            if (node_task.check_network(public_ip) and
                    node_task.check_network(cluster_ip)):
                res['check_network'] = True
            else:
                res['check_network'] = False
        # TODO: check roles
        if "roles" in items:
            roles = data.get("roles") or None
            if roles:
                roles = roles.split(',')
            services = node_task.probe_node_services()
            res['check_roles'] = self._node_check_roles(roles, services)
        if "firewall" in items:
            res['check_firewall'] = node_task.check_firewall()
        if "container" in items:
            res['check_container'] = node_task.check_container()
        return res

    def nodes_inclusion_check(self, ctxt, datas):
        logger.info("check datas: %s", datas)
        status = {}
        status['leak_admin_ips'] = self._nodes_inclusion_check_admin_ips(
            ctxt, datas)
        logger.info("leak_admin_ips: %s", status['leak_admin_ips'])
        leaks, extras = self._nodes_inclusion_check_all_ips(
            ctxt, datas)
        status['leak_cluster_ips'] = leaks
        status['extra_cluster_ips'] = extras
        logger.info("leak_cluster_ips: %s", status['leak_admin_ips'])
        status['nodes'] = []
        for data in datas:
            admin_ip = data.get('ip_address')
            res = self._node_check(ctxt, data, [
                "hostname",
                "selinux",
                "network",
                "roles",
                "athena_ports",
                "firewall",
                "container"
            ])
            res['admin_ip'] = admin_ip
            status['nodes'].append(res)
            logger.info("check node: %s, result: %s", admin_ip, res)
        for node in status['nodes']:
            if node['admin_ip'] not in extras:
                node['check_Installation_package'] = True
        return status

    def node_reporter(self, ctxt, node_summary, node_id):
        logger.info("node_reporter: %s", node_summary)
        node = objects.Node.get_by_id(ctxt, node_id)
        node.cpu_num = node_summary.get("cpu_num")
        node.cpu_model = node_summary.get("cpu_model")
        node.cpu_core_num = node_summary.get("cpu_core_num")
        node.vendor = node_summary.get("vendor")
        node.sys_type = node_summary.get("sys_type")
        node.sys_version = node_summary.get("sys_version")
        node.mem_size = node_summary.get("memsize")
        node.save()
