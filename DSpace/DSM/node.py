import json

import six
from oslo_log import log as logging

from DSpace import exception as exc
from DSpace import objects
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
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
            node_task.prometheus_target_config(action='remove',
                                               service='node_exporter')
            self._remove_node_resource(ctxt, node)

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

        node_task.prometheus_target_config(action='add',
                                           service='node_exporter')

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
        check_items = ["hostname", "selinux", "ceph_ports", "ceph_package",
                       "network", "athena_ports", "firewall"]
        # TODO delete it
        data['ip_address'] = data.get('admin_ip')
        res = self._node_check(ctxt, data, check_items)
        return res

    def nodes_inclusion(self, ctxt, datas):
        logger.debug("include nodes: {}", datas)
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
        self.task_submit(include_flow, ctxt, t, datas)
        return t

    def nodes_inclusion_clean(self, ctxt):
        logger.debug("include delete nodes")
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
        self.task_submit(include_clean_flow, ctxt, t)
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
        nodes = objects.NodeList.get_all(
            ctxt,
            filters={
                "ip_address": admin_ip
            }
        )
        res['check_admin_ip'] = False if nodes else True
        # cluster_ip
        nodes = objects.NodeList.get_all(
            ctxt,
            filters={
                "cluster_ip": cluster_ip
            }
        )
        res['check_cluster_ip'] = False if nodes else True
        # public_ip
        nodes = objects.NodeList.get_all(
            ctxt,
            filters={
                "public_ip": public_ip
            }
        )
        res['check_public_ip'] = False if nodes else True
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
            ports = ["6789", "9876", "9100", "9283", "7480"]
            res['check_ceph_port'] = self._node_check_port(node_task, ports)
            # TODO: delete it
            res['check_port'] = self._node_check_port(node_task, ports)
        if "athena_ports" in items:
            # TODO: move to db
            ports = ["9100", "2083"]
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
                "ceph_ports",
                "ceph_package",
                "network",
                "roles",
                "athena_ports",
                "firewall"
            ])
            res['admin_ip'] = admin_ip
            status['nodes'].append(res)
            logger.info("check node: %s, result: %s", admin_ip, res)
        for node in status['nodes']:
            if node['admin_ip'] not in extras:
                node['check_Installation_package'] = True
        return status
