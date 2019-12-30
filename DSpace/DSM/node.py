import base64
import os
import struct
import time
import uuid

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
from DSpace.objects.fields import ConfigKey
from DSpace.taskflows.include import InclusionNodesCheck
from DSpace.taskflows.include import include_clean_flow
from DSpace.taskflows.include import include_flow
from DSpace.taskflows.node import NodeMixin
from DSpace.taskflows.node import NodesCheck
from DSpace.taskflows.node import NodeTask
from DSpace.taskflows.node import PrometheusTargetMixin
from DSpace.tools.prometheus import PrometheusTool
from DSpace.utils import cluster_config

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
        node_task = NodeTask(ctxt, node)

        try:
            if node.role_monitor:
                if self._mon_uninstall(ctxt, node) != 'success':
                    raise exc.NodeRolesUpdateError(node=node.hostname)
            if node.role_storage:
                if self._storage_uninstall(ctxt, node) != 'success':
                    raise exc.NodeRolesUpdateError(node=node.hostname)
            if node.role_block_gateway:
                if self._bgw_uninstall(ctxt, node) != 'success':
                    raise exc.NodeRolesUpdateError(node=node.hostname)
            if node.role_object_gateway:
                if self._rgw_uninstall(ctxt, node) != 'success':
                    raise exc.NodeRolesUpdateError(node=node.hostname)

            node_task.ceph_package_uninstall()
            node_task.chrony_uninstall()
            node_task.node_exporter_uninstall()
            node_task.dspace_agent_uninstall()
            node_task.router_images_uninstall()
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
                           node, status, err_msg=err_msg)

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
                                         Action.NODE_UPDATE_RACK, node)
        node.rack_id = rack_id
        node.save()
        self.finish_action(begin_action, node_id, node.hostname, node)
        return node

    def _node_delete_check(self, ctxt, node):
        allowed_status = [s_fields.NodeStatus.ACTIVE,
                          s_fields.NodeStatus.WARNING,
                          s_fields.NodeStatus.ERROR]
        if node.status not in allowed_status:
            raise exc.InvalidInput(_("Node status not allow!"))
        if node.role_admin:
            raise exc.InvalidInput(_("admin role could not delete!"))
        # TODO concurrence not support now, need remove when support
        roles_status = [s_fields.NodeStatus.CREATING,
                        s_fields.NodeStatus.DELETING,
                        s_fields.NodeStatus.DEPLOYING_ROLE,
                        s_fields.NodeStatus.REMOVING_ROLE]
        nodes = objects.NodeList.get_all(
            ctxt, filters={'status': roles_status})
        if len(nodes) and node.role_monitor:
            raise exc.InvalidInput(_("Only one node can set roles at the"
                                     " same time"))
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

        if node.role_object_gateway:
            self._rgw_uninstall_check(ctxt, node)

    def node_delete(self, ctxt, node_id):
        node = objects.Node.get_by_id(ctxt, node_id)
        # judge node could be delete
        self._node_delete_check(ctxt, node)
        begin_action = self.begin_action(
            ctxt, Resource.NODE, Action.DELETE, node)
        node.status = s_fields.NodeStatus.DELETING
        node.save()
        self.task_submit(self._node_delete, ctxt, node, begin_action)
        return node

    def _node_get_metrics_overall(self, ctxt, nodes):
        # TODO get all data at once
        prometheus = PrometheusTool(ctxt)
        for node in nodes:
            if (node.status not in [s_fields.NodeStatus.CREATING,
                                    s_fields.NodeStatus.DELETING]):
                prometheus.node_get_metrics_overall(node)

    def _filter_gateway_network(self, ctxt, nodes):
        gateway_cidr = objects.sysconfig.sys_config_get(
            ctxt, key="gateway_cidr")
        for node in nodes:
            nets = []
            for net in node.networks:
                node.networks = []
                if not net.ip_address:
                    continue
                if net.ip_address in IPNetwork(gateway_cidr):
                    nets.append(net)
            node.networks = nets
            rgws = []
            for rgw in node.radosgws:
                if not rgw.router_id:
                    rgws.append(rgw)
            node.radosgws = rgws

    def _filter_by_routers(self, ctxt, nodes):
        n = []
        for node in nodes:
            rgw_router = objects.RadosgwRouterList.get_all(
                ctxt, filters={'node_id': node.id})
            if not rgw_router:
                n.append(node)
        return n

    def node_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                     sort_dirs=None, filters=None, offset=None,
                     expected_attrs=None):
        if filters.get('role_object_gateway'):
            if expected_attrs:
                expected_attrs.append('radosgws')
        no_router = 0
        if 'no_router' in filters:
            no_router = filters.pop('no_router')
        nodes = objects.NodeList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)
        if filters.get('role_object_gateway'):
            self._filter_gateway_network(ctxt, nodes)
        if no_router:
            nodes = self._filter_by_routers(ctxt, nodes)
        self._node_get_metrics_overall(ctxt, nodes)

        return nodes

    def node_get_count(self, ctxt, filters=None):
        return objects.NodeList.get_count(ctxt, filters=filters)

    def _set_ceph_conf(self, ctxt, key, value, value_type, group=None,
                       display_description=None):
        group = group or "global"
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
            ctxt, ConfigKey.MAX_MONITOR_NUM
        )
        mon_num = objects.NodeList.get_count(
            ctxt, filters={"role_monitor": True}
        )
        if not public_network or not cluster_network:
            raise exc.InvalidInput(_("Please set network planning"))
        if mon_num >= max_mon_num:
            raise exc.InvalidInput(_("Max monitor num is %s") % max_mon_num)

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
            raise exc.InvalidInput(_("Node %s has osd!") % node.hostname)
        node_accelerate_disks = objects.DiskList.get_count(
            ctxt,
            filters={"node_id": node.id, "role": s_fields.DiskRole.ACCELERATE}
        )
        if node_accelerate_disks:
            raise exc.InvalidInput(_("Node %s has accelerate "
                                     "disk!") % node.hostname)

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
        filters = {"node_id": node.id}
        node_rgws = objects.RadosgwList.get_count(ctxt, filters=filters)
        if node_rgws:
            raise exc.InvalidInput(_("Node %s has radosgw!") % node.hostname)
        router_service = objects.RouterServiceList.get_all(
            ctxt, filters=filters)
        if router_service:
            raise exc.InvalidInput(
                _("Node %s has radosgw router!") % node.hostname)

    def _bgw_install_check(self, ctxt, node):
        if node.role_block_gateway:
            raise exc.InvalidInput(_(
                "The block gateway role has been installed."))

    def _bgw_uninstall_check(self, ctxt, node):
        if not node.role_block_gateway:
            raise exc.InvalidInput(_(
                "The block gateway role has not yet been installed"))

    def _generate_mon_secret(self):
        key = os.urandom(16)
        header = struct.pack('<hiih', 1, int(time.time()), 0, len(key))
        mon_secret = base64.b64encode(header + key).decode()
        logger.info("Generate mon secret for first mon: <%s>", mon_secret)
        return mon_secret

    def _save_admin_keyring(self, ctxt, node_task):
        # Save  client.admin keyring to DB
        admin_keyring = node_task.collect_keyring("client.admin")
        if not admin_keyring:
            logger.error("no admin keyring, clients can't connect to cluster")
            raise exc.ProgrammingError("items empty!")
        else:
            self._set_ceph_conf(ctxt,
                                group="keyring",
                                key="client.admin",
                                value=admin_keyring,
                                value_type="string")
            logger.info("create or update client.admin: <%s>", admin_keyring)

    def _init_ceph_config(self, ctxt, enable_cephx=False):
        logger.info("init ceph config before install first monitor")
        public_network = objects.sysconfig.sys_config_get(
            ctxt, key="public_cidr"
        )
        cluster_network = objects.sysconfig.sys_config_get(
            ctxt, key="cluster_cidr"
        )
        fsid = str(uuid.uuid4())
        init_configs = {
            'fsid': {'type': 'string', 'value': fsid},
            'osd_objectstore': {'type': 'string', 'value': 'bluestore',
                                'group': 'mon'},
            'public_network': {'type': 'string', 'value': public_network},
            'cluster_network': {'type': 'string', 'value': cluster_network}
        }
        init_configs.update(cluster_config.default_cluster_configs)
        mon_secret = None
        if enable_cephx:
            mon_secret = self._generate_mon_secret()
            self._set_ceph_conf(ctxt,
                                group="keyring",
                                key="mon.",
                                value=mon_secret,
                                value_type="string")
            init_configs.update(cluster_config.auth_cephx_config)
        else:
            init_configs.update(cluster_config.auth_none_config)
        for key, value in init_configs.items():
            self._set_ceph_conf(ctxt,
                                group=value.get('group'),
                                key=key,
                                value=value.get('value'),
                                value_type=value.get('type'))

    def _sync_ceph_configs(self, ctxt):
        mon_nodes = objects.NodeList.get_all(
            ctxt, filters={"role_monitor": True}
        )
        mon_host = ",".join(
            [str(n.public_ip) for n in mon_nodes]
        )
        mon_initial_members = ",".join([n.hostname for n in mon_nodes])
        self._set_ceph_conf(
            ctxt, key="mon_host", value=mon_host, value_type='string')
        self._set_ceph_conf(
            ctxt, key="mon_initial_members",
            value=mon_initial_members, value_type='string')
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

    def _create_mon_service(self, ctxt, node):
        logger.debug("Create service for mon and mgr in database")
        for name in ["MON", "MGR", "MDS"]:
            service = objects.Service(
                ctxt, name=name, status=s_fields.ServiceStatus.ACTIVE,
                node_id=node.id, cluster_id=ctxt.cluster_id, counter=0,
                role="role_monitor"
            )
            service.create()

    def _mon_install(self, ctxt, node):
        logger.info("mon install on node %s, ip:%s", node.id, node.ip_address)
        begin_action = self.begin_action(
            ctxt, Resource.NODE, Action.SET_ROLES, node)
        mon_nodes = objects.NodeList.get_all(
            ctxt, filters={"role_monitor": True}
        )
        mon_nodes = list(mon_nodes)
        mon_nodes.append(node)
        node_task = NodeTask(ctxt, node)
        enable_cephx = objects.sysconfig.sys_config_get(
            ctxt, key=ConfigKey.ENABLE_CEPHX
        )
        try:
            if len(mon_nodes) == 1:
                self._init_ceph_config(ctxt, enable_cephx=enable_cephx)
            mon_host = ",".join(
                [str(n.public_ip) for n in mon_nodes]
            )
            mon_initial_members = ",".join([n.hostname for n in mon_nodes])
            self._set_ceph_conf(
                ctxt, key="mon_host", value=mon_host, value_type='string')
            self._set_ceph_conf(
                ctxt, key="mon_initial_members", value=mon_initial_members,
                value_type='string')
            mon_secret = None
            if enable_cephx:
                db_mon_secret = objects.CephConfig.get_by_key(
                    ctxt, 'keyring', 'mon.')
                mon_secret = db_mon_secret.value
            node_task.ceph_mon_install(mon_secret)
            if enable_cephx:
                self._save_admin_keyring(ctxt, node_task)
            node.role_monitor = True
            node.save()
            PrometheusTargetMixin().target_add(ctxt, node, service='mgr')
            self._sync_ceph_configs(ctxt)
            self._create_mon_service(ctxt, node)
            msg = _("node %s: set mon role success") % node.hostname
            op_status = "SET_ROLE_MON_SUCCESS"
            status = 'success'
            err_msg = None
        except Exception as e:
            logger.exception('set mon role failed %s', e)
            # restore monitor
            node_task.ceph_mon_uninstall()
            node.role_monitor = False
            node.save()
            mon_nodes.remove(node)
            mon_host = ",".join(
                [str(n.public_ip) for n in mon_nodes]
            )
            mon_initial_members = ",".join([n.hostname for n in mon_nodes])
            self._set_ceph_conf(
                ctxt, key="mon_host", value=mon_host, value_type='string')
            self._set_ceph_conf(
                ctxt, key="mon_initial_members", value=mon_initial_members,
                value_type='string')
            msg = _("node %s: set mon role error") % node.hostname
            op_status = "SET_ROLE_MON_ERROR"
            status = 'fail'
            err_msg = str(e)
        # send ws message
        self.finish_action(begin_action, node.id, node.hostname,
                           node, status, err_msg=err_msg)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, node, op_status, msg)
        return status

    def _remove_mon_services(self, ctxt, node):
        services = objects.ServiceList.get_all(
            ctxt, filters={"node_id": node.id, "role": "role_monitor"})
        for service in services:
            service.destroy()

    def _mon_uninstall(self, ctxt, node):
        logger.info("mon uninstall on node %s, ip:%s",
                    node.id, node.ip_address)
        begin_action = self.begin_action(
            ctxt, Resource.NODE, Action.SET_ROLES, node)
        task = NodeTask(ctxt, node)
        try:
            self._remove_mon_services(ctxt, node)
            task.ceph_mon_uninstall()
            node.role_monitor = False
            node.save()
            self._sync_ceph_configs(ctxt)
            msg = _("node %s: unset mon role success") % node.hostname
            op_status = "UNSET_ROLE_MON_SUCCESS"
            PrometheusTargetMixin().target_remove(ctxt, node, service='mgr')
            status = 'success'
            err_msg = None
        except Exception as e:
            logger.exception('unset mon role failed %s', e)
            msg = _("node %s: unset mon role error") % node.hostname
            op_status = "UNSET_ROLE_MON_ERROR"
            status = 'fail'
            err_msg = str(e)
        # send ws message
        self.finish_action(begin_action, node.id, node.hostname,
                           node, status, err_msg=err_msg)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, node, op_status, msg)
        return status

    def _storage_install(self, ctxt, node):
        logger.info("storage install on node %s, ip:%s",
                    node.id, node.ip_address)
        begin_action = self.begin_action(
            ctxt, Resource.NODE, Action.SET_ROLES, node)
        node_task = NodeTask(ctxt, node)
        try:
            node_task.ceph_osd_package_install()
            node.role_storage = True
            node.save()
            msg = _("node %s: set storage role success") % node.hostname
            op_status = "SET_ROLE_STORAGE_SUCCESS"
            status = 'success'
            err_msg = None
        except Exception as e:
            logger.exception('set storage role failed %s', e)
            node_task.ceph_osd_package_uninstall()
            msg = _("node %s: set storage role error") % node.hostname
            op_status = "SET_ROLE_STORAGE_ERROR"
            status = 'fail'
            err_msg = str(e)
        # send ws message
        self.finish_action(begin_action, node.id, node.hostname,
                           node, status, err_msg=err_msg)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, node, op_status, msg)
        return status

    def _storage_uninstall(self, ctxt, node):
        logger.info("storage uninstall on node %s, ip:%s",
                    node.id, node.ip_address)
        begin_action = self.begin_action(
            ctxt, Resource.NODE, Action.SET_ROLES, node)
        try:
            node_task = NodeTask(ctxt, node)
            node_task.ceph_osd_package_uninstall()
            node.role_storage = False
            node.save()
            msg = _("node %s: unset storage role success") % node.hostname
            op_status = "UNSET_ROLE_STORAGE_SUCCESS"
            status = 'success'
            err_msg = None
        except Exception as e:
            logger.exception('unset storage role failed %s', e)
            msg = _("node %s: unset storage role error") % node.hostname
            op_status = "UNSET_ROLE_STORAGE_ERROR"
            status = 'fail'
            err_msg = str(e)
        # send ws message
        self.finish_action(begin_action, node.id, node.hostname,
                           node, status, err_msg=err_msg)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, node, op_status, msg)
        return status

    def _mds_install(self, ctxt, node):
        pass

    def _mds_uninstall(self, ctxt, node):
        pass

    def _rgw_install(self, ctxt, node):
        logger.info("rgw install on node %s, ip:%s",
                    node.id, node.ip_address)
        begin_action = self.begin_action(
            ctxt, Resource.NODE, Action.SET_ROLES, node)
        node_task = NodeTask(ctxt, node)
        try:
            node_task.ceph_rgw_package_install()
            node.role_object_gateway = True
            node.save()
            msg = _("node %s: set rgw role success") % node.hostname
            op_status = "SET_ROLE_RGW_SUCCESS"
            status = 'success'
            err_msg = None
        except Exception as e:
            logger.exception('set rgw role failed %s', e)
            node_task.ceph_rgw_package_uninstall()
            msg = _("node %s: set rgw role error") % node.hostname
            op_status = "SET_ROLE_RGW_ERROR"
            status = 'fail'
            err_msg = str(e)
        # send ws message
        self.finish_action(begin_action, node.id, node.hostname,
                           node, status, err_msg=err_msg)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, node, op_status, msg)
        return status

    def _rgw_uninstall(self, ctxt, node):
        logger.info("rgw uninstall on node %s, ip:%s",
                    node.id, node.ip_address)
        begin_action = self.begin_action(
            ctxt, Resource.NODE, Action.SET_ROLES, node)
        try:
            node_task = NodeTask(ctxt, node)
            node_task.ceph_rgw_package_uninstall()
            node.role_object_gateway = False
            node.save()
            msg = _("node %s: unset rgw role success") % node.hostname
            op_status = "UNSET_ROLE_RGW_SUCCESS"
            status = 'success'
            err_msg = None
        except Exception as e:
            logger.exception('unset rgw role failed %s', e)
            msg = _("node %s: unset rgw role error") % node.hostname
            op_status = "UNSET_ROLE_RGW_ERROR"
            status = 'fail'
            err_msg = str(e)
        # send ws message
        self.finish_action(begin_action, node.id, node.hostname,
                           node, status, err_msg=err_msg)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, node, op_status, msg)
        return status

    def _bgw_install(self, ctxt, node):
        node.role_block_gateway = True
        node.save()
        return node

    def _bgw_uninstall(self, ctxt, node):
        node.role_block_gateway = False
        node.save()
        return node

    def _notify_dsa_update(self, ctxt, node):
        logger.info("Send node update info to %s", node.id)
        node = objects.Node.get_by_id(ctxt, node.id)
        self.notify_node_update(ctxt, node)

    def _node_roles_set(self, ctxt, node, i_roles, u_roles):
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
                node.status = s_fields.NodeStatus.DEPLOYING_ROLE
                node.save()
                func = install_role_map.get(role)
                func(ctxt, node)
            for role in u_roles:
                node.status = s_fields.NodeStatus.REMOVING_ROLE
                node.save()
                func = uninstall_role_map.get(role)
                func(ctxt, node)
            logger.info('set node roles success')
            msg = _("node %s: set roles success") % node.hostname
            op_status = "SET_ROLES_SUCCESS"
        except Exception as e:
            logger.exception('set node roles failed %s', e)
            msg = _("node %s: set roles error") % node.hostname
            op_status = "SET_ROLES_ERROR"
        node.status = s_fields.NodeStatus.ACTIVE
        node.save()
        # notify dsa to update node info
        try:
            self._notify_dsa_update(ctxt, node)
        except Exception as e:
            logger.error("Update dsa node info failed: %s", e)

        # send ws message
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, node, op_status, msg)

    def node_roles_set(self, ctxt, node_id, data):
        # TODO concurrence not support now, need remove when support
        roles_status = [s_fields.NodeStatus.CREATING,
                        s_fields.NodeStatus.DELETING,
                        s_fields.NodeStatus.DEPLOYING_ROLE,
                        s_fields.NodeStatus.REMOVING_ROLE]
        nodes = objects.NodeList.get_all(
            ctxt, filters={'status': roles_status})
        if len(nodes):
            raise exc.InvalidInput(_("Only one node can set roles at the"
                                     " same time"))

        node = objects.Node.get_by_id(ctxt, node_id)
        allowed_status = [s_fields.NodeStatus.ACTIVE,
                          s_fields.NodeStatus.WARNING]
        if node.status not in allowed_status:
            raise exc.InvalidInput(_("Only host's status is active can set "
                                     "role(host_name: %s)") % node.hostname)
        self.check_agent_available(ctxt, node)
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
        node.save()
        logger.info('node %s roles check pass', node.hostname)
        self.task_submit(self._node_roles_set, ctxt, node, i_roles, u_roles)
        return node

    def _node_create(self, ctxt, node, data):
        begin_action = self.begin_action(ctxt, Resource.NODE, Action.CREATE)
        node_task = NodeTask(ctxt, node)
        try:
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
            node.disks = objects.DiskList.get_all(
                ctxt, filters={"node_id": node.id})
            node.status = s_fields.NodeStatus.ACTIVE
            self._node_get_metrics_overall(ctxt, [node])
            logger.info('create node success, node ip: {}'.format(
                        node.ip_address))
            msg = _("node {} create success").format(node.hostname)
            op_status = "CREATE_SUCCESS"
            err_msg = None
        except Exception as e:
            node.status = s_fields.NodeStatus.ERROR
            logger.exception('create node error, node ip: %s, reason: %s',
                             node.ip_address, e)
            msg = _("node {} create error").format(node.hostname)
            err_msg = str(e)
            op_status = "CREATE_ERROR"
        node.save()

        # notify dsa to update node info
        try:
            self._notify_dsa_update(ctxt, node)
        except Exception as e:
            logger.error("Update dsa node info failed: %s", e)

        # send ws message
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, node, op_status, msg)
        self.finish_action(begin_action, node.id, node.hostname,
                           node, node.status, err_msg=err_msg)
        return node

    def node_create(self, ctxt, data):
        logger.debug("add node to cluster {}".format(data.get('ip_address')))

        NodeMixin._check_node_ip_address(ctxt, data)
        # TODO concurrence not support now, need remove when support
        roles_status = [s_fields.NodeStatus.CREATING,
                        s_fields.NodeStatus.DELETING,
                        s_fields.NodeStatus.DEPLOYING_ROLE,
                        s_fields.NodeStatus.REMOVING_ROLE]
        nodes = objects.NodeList.get_all(
            ctxt, filters={'status': roles_status})
        roles = data.get('roles', "").split(',')
        role_monitor = "monitor" in roles
        if len(nodes) and role_monitor:
            raise exc.InvalidInput(_("Only one node can set roles at the"
                                     " same time"))
        if role_monitor:
            self._mon_install_check(ctxt)

        node = objects.Node(
            ctxt, ip_address=data.get('ip_address'),
            hostname=data.get('hostname'),
            password=data.get('password'),
            cluster_ip=data.get('cluster_ip'),
            public_ip=data.get('public_ip'),
            object_gateway_ip_address=data.get('gateway_ip'),
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

    def nodes_check(self, ctxt, data):
        checker = NodesCheck(ctxt)
        res = checker.check(data)
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
        mon_num = objects.NodeList.get_count(
            ctxt, filters={"role_monitor": True}
        )
        if mon_num:
            raise exc.InvalidInput(_("Please create new cluster to import"))
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
                           cluster, status, err_msg=err_msg)

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
                           cluster, status, err_msg=err_msg)

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

    def nodes_inclusion_check(self, ctxt, datas):
        logger.info("check datas: %s", datas)
        checker = InclusionNodesCheck(ctxt)
        res = checker.check(datas)
        return res

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
