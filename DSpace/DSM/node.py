import json

import six
from oslo_log import log as logging

from DSpace import exception as exc
from DSpace import objects
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.taskflows.node import NodeMixin
from DSpace.taskflows.node import NodeTask
from DSpace.tools.prometheus import PrometheusTool
from DSpace.utils import cluster_config as ClusterConfg

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

    def _node_delete(self, ctxt, node):
        node_id = node.id
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
            logger.info("node delete success")
            msg = _("Node removed!")
        except Exception as e:
            node.status = s_fields.NodeStatus.ERROR
            node.save()
            logger.exception("node delete error: %s", e)
            msg = _("Node remove error!")

        node = objects.Node.get_by_id(ctxt, node_id)
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, node, "DELETED", msg)

    def node_update_rack(self, ctxt, node_id, rack_id):
        node = objects.Node.get_by_id(ctxt, node_id)
        node.rack_id = rack_id
        node.save()
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
            ctxt, filters={"node_id": node.id}
        )
        if disk_partition_num:
            raise exc.InvalidInput(_("Please remove disk partition first!"))

    def node_delete(self, ctxt, node_id):
        node = objects.Node.get_by_id(ctxt, node_id)
        # judge node could be delete
        self._node_delete_check(ctxt, node)
        node.status = s_fields.NodeStatus.DELETING
        node.save()
        self.task_submit(self._node_delete, ctxt, node)
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

    def _mon_install_check(self, ctxt):
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
        osd_num = objects.OsdList.get_count(ctxt)
        mon_num = objects.NodeList.get_count(
            ctxt, filters={"role_monitor": True}
        )
        if osd_num and mon_num == 1:
            raise exc.InvalidInput(_("Please remove osd first!"))

    def _storage_install_check(self, ctxt):
        pass

    def _storage_uninstall_check(self, ctxt, node):
        node_osds = objects.OsdList.get_count(
            ctxt, filters={"node_id": node.id}
        )
        if node_osds:
            raise exc.InvalidInput(_("Node %s has osd!" % node.hostname))

    def _mds_install_check(self, ctxt):
        pass

    def _mds_uninstall_check(self, ctxt, node):
        pass

    def _rgw_install_check(self, ctxt):
        pass

    def _rgw_uninstall_check(self, ctxt, node):
        pass

    def _bgw_install_check(self, ctxt):
        pass

    def _bgw_uninstall_check(self, ctxt, node):
        pass

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
            new_cluster_config = {
                'fsid': {'type': 'string', 'value': ctxt.cluster_id},
                'mon_host': {'type': 'string',
                             'value': str(node.public_ip)},
                'mon_initial_members': {'type': 'string',
                                        'value': node.hostname},
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
        else:
            task.ceph_mon_uninstall(last_mon=True)
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
                func = install_role_map.get(role)
                func(ctxt, node)
            for role in u_roles:
                func = uninstall_role_map.get(role)
                func(ctxt, node)
            status = s_fields.NodeStatus.ACTIVE
            logger.info('set node roles success')
            msg = _("set node roles")
        except Exception as e:
            status = s_fields.NodeStatus.ERROR
            logger.exception('set node roles failed %s', e)
            msg = _("set node roles error, reason: {}".format(str(e)))
        node.status = status
        node.save()
        # send ws message
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, node, "DEPLOYED", msg)

    def node_roles_set(self, ctxt, node_id, data):
        node = objects.Node.get_by_id(ctxt, node_id)
        if node.status != s_fields.NodeStatus.ACTIVE:
            raise exc.InvalidInput(_("Only host's status is active can set"
                                     "role(host_name: %s)" % node.hostname))
        i_roles = data.get('install_roles')
        u_roles = data.get('uninstall_roles')
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
            func(ctxt)
        for role in u_roles:
            func = uninstall_check_role_map.get(role)
            func(ctxt, node)

        logger.info('node %s roles check pass', node.hostname)
        self.task_submit(self._node_roles_set, ctxt, node, i_roles, u_roles)
        return node

    def _node_create(self, ctxt, node, data):
        try:
            node_task = NodeTask(ctxt, node)
            node_task.dspace_agent_install()
            node_task.chrony_install()
            node_task.node_exporter_install()
            # add agent rpc service
            agent_port = objects.sysconfig.sys_config_get(
                ctxt, "agent_port")
            endpoint = {"ip": str(node.ip_address), "port": agent_port}
            rpc_service = objects.RPCService(
                ctxt, service_name='agent',
                hostname=node.hostname,
                endpoint=json.dumps(endpoint),
                cluster_id=node.cluster_id,
                node_id=node.id)
            rpc_service.create()

            node_task.wait_agent_ready()
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
        except Exception as e:
            status = s_fields.NodeStatus.ERROR
            logger.exception('create node error, node ip: %s, reason: %s',
                             node.ip_address, e)
            msg = _("node create error, reason: {}".format(str(e)))
        node.status = status
        node.save()

        # send ws message
        wb_client = WebSocketClientManager(context=ctxt).get_client()
        wb_client.send_message(ctxt, node, "CREATED", msg)
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
            gateway_ip_address=data.get('gateway_ip_address'),
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

    def _validate_ip(self, ip_str):
        sep = ip_str.split('.')
        if len(sep) != 4:
            raise exc.Invalid(_('IP address {} format'
                                'is incorrect!').format(ip_str))
        for i, x in enumerate(sep):
            int_x = int(x)
            if int_x < 0 or int_x > 255:
                raise exc.Invalid(_('IP address {} format'
                                    'is incorrect!').format(ip_str))
        return True

    def node_check(self, ctxt, data):
        logger.debug("check node: {}".format(data.get('admin_ip')))

        ip_dict = {}
        ip_dict['check_through'] = True
        node = objects.Node(
            ctxt, ip_address=data.get('admin_ip'),
            password=data.get('password'))

        admin_ip = data.get('admin_ip')
        public_ip = data.get('public_ip')
        cluster_ip = data.get('cluster_ip')
        node_task = NodeTask(ctxt, node)
        node_infos = node_task.node_get_infos()
        hostname = node_infos.get('hostname')
        if not hostname:
            ip_dict['check_through'] = False
            return ip_dict

        li_ip = [admin_ip, public_ip, cluster_ip]
        if not all(li_ip):
            raise exc.Invalid(_('admin_ip,cluster_ip,public_ip is required'))
        for ip in li_ip:
            self._validate_ip(ip)
        ip_dict['check_admin_ip'] = True
        ip_dict['check_hostname'] = True
        ip_dict['check_gateway_ip'] = True
        ip_dict['check_cluster_ip'] = True
        ip_dict['check_public_ip'] = True
        if objects.NodeList.get_all(ctxt, filters={"ip_address": admin_ip}):
            ip_dict['check_admin_ip'] = False
        if objects.NodeList.get_all(
            ctxt,
            filters={
                "cluster_ip": cluster_ip
            }
        ):
            ip_dict['check_cluster_ip'] = False
        if objects.NodeList.get_all(
            ctxt,
            filters={
                "public_ip": public_ip
            }
        ):
            ip_dict['check_public_ip'] = False
        if objects.NodeList.get_all(ctxt, filters={"hostname": hostname}):
            ip_dict['check_hostname'] = False
        port = ["6789", "9876", "9100", "9283", "7480"]
        ip_dict['check_port'] = []
        for po in port:
            if not node_task.check_port(po):
                ip_dict['check_port'].append({"port": po, "status": False})
            else:
                ip_dict['check_port'].append({"port": po, "status": True})
        ip_dict['check_SELinux'] = node_task.check_selinux()
        if node_task.check_ceph_is_installed():
            ip_dict['check_Installation_package'] = False
        else:
            ip_dict['check_Installation_package'] = True
        if (node_task.check_network(public_ip) and
                node_task.check_network(cluster_ip)):
            ip_dict['check_network'] = True
        else:
            ip_dict['check_network'] = True

        return ip_dict
