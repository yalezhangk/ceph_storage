import six
from oslo_log import log as logging

from DSpace import objects
from DSpace.DSI.wsclient import WebSocketClientManager
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.taskflows.node import NodeMixin
from DSpace.taskflows.node import NodeTask
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

    def node_delete(self, ctxt, node_id):
        node = objects.Node.get_by_id(ctxt, node_id)
        node.destroy()
        return node

    def node_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                     sort_dirs=None, filters=None, offset=None,
                     expected_attrs=None):
        nodes = objects.NodeList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs)
        return nodes

    def _set_ceph_conf(self, ctxt, key, value, group="global",
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
                display_description=display_description,
                cluster_id=ctxt.cluster_id
            )
            cephconf.create()
        else:
            cephconf = cephconf[0]
            cephconf.value = value
            cephconf.save()
        return cephconf

    def _mon_install(self, ctxt, node):
        node.status = s_fields.NodeStatus.DEPLOYING
        node.role_monitor = True
        node.save()

        mon_nodes = objects.NodeList.get_all(
            ctxt, filters={"cluster_id": ctxt.cluster_id,
                           "role_monitor": True}
        )
        if len(mon_nodes) == 1:
            # init ceph config
            public_network = objects.sysconfig.sys_config_get(
                ctxt, key="admin_cidr"
            )
            cluster_network = objects.sysconfig.sys_config_get(
                ctxt, key="admin_cidr"
            )
            new_cluster_config = {
                'fsid': ctxt.cluster_id,
                'mon_host': str(node.storage_public_ip_address),
                'mon_initial_members': node.hostname,
                'public_network': public_network,
                'cluster_network': cluster_network
            }
            configs = {}
            configs.update(ClusterConfg.default_cluster_configs)
            configs.update(new_cluster_config)
            for key, value in configs.items():
                self._set_ceph_conf(ctxt, key=key, value=value)
        else:
            mon_host = ",".join(
                [n.storage_public_ip_address for n in mon_nodes]
            )
            mon_initial_members = ",".join([n.hostname for n in mon_nodes])
            self._set_ceph_conf(ctxt, key="mon_host", value=mon_host)
            self._set_ceph_conf(ctxt,
                                key="mon_initial_members",
                                value=mon_initial_members)
        node_task = NodeTask(ctxt, node)
        node_task.ceph_mon_install()
        return node

    def _mon_uninstall(self, ctxt, node):
        pass

    def _storage_install(self, ctxt, node):
        pass

    def _storage_uninstall(self, ctxt, node):
        pass

    def _mds_install(self, ctxt, node):
        pass

    def _mds_uninstall(self, ctxt, node):
        pass

    def _rgw_install(self, ctxt, node):
        pass

    def _rgw_uninstall(self, ctxt, node):
        pass

    def _bgw_install(self, ctxt, node):
        pass

    def _bgw_uninstall(self, ctxt, node):
        pass

    def _node_roles_set(self, ctxt, node, data):

        i_roles = data.get('install_roles')
        u_roles = data.get('uninstall_roles')
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
            logger.error('set node roles failed')
            msg = _("set node roles error, reason: {}".format(str(e)))
        node.status = status
        node.save()
        # send ws message
        wb_client = WebSocketClientManager(
            ctxt, cluster_id=node.cluster_id
        ).get_client()
        wb_client.send_message(ctxt, node, "DEPLOYED", msg)

    def node_roles_set(self, ctxt, node_id, data):
        node = objects.Node.get_by_id(ctxt, node_id)
        node.status = s_fields.NodeStatus.DEPLOYING
        node.save()

        self.executor.submit(self._node_roles_set, ctxt, node, data)
        return node

    def _node_create(self, ctxt, node, data):
        try:
            node_task = NodeTask(ctxt, node)
            node_task.dspace_agent_install()
            node_task.chrony_install()
            node_task.node_exporter_install()
            status = s_fields.NodeStatus.ACTIVE
            logger.info('create node success, node ip: {}'.format(
                        node.ip_address))
            msg = _("node create success")
        except Exception as e:
            status = s_fields.NodeStatus.ERROR
            logger.error('create node error, node ip: {}, reason: {}'.format(
                         node.ip_address, str(e)))
            msg = _("node create error, reason: {}".format(str(e)))
        node.status = status
        node.save()

        roles = data.get('roles', "").split(',')
        role_monitor = "monitor" in roles
        role_storage = "storage" in roles
        if role_monitor:
            self._mon_install(ctxt, node)
        if role_storage:
            self._storage_install(ctxt, node)
        # send ws message
        wb_client = WebSocketClientManager(
            ctxt, cluster_id=node.cluster_id
        ).get_client()
        wb_client.send_message(ctxt, node, "CREATED", msg)
        return node

    def node_create(self, ctxt, data):
        logger.debug("add node to cluster {}".format(data.get('ip_address')))

        NodeMixin._check_node_ip_address(ctxt, data)

        node = objects.Node(
            ctxt, ip_address=data.get('ip_address'),
            hostname=data.get('hostname'),
            password=data.get('password'),
            gateway_ip_address=data.get('gateway_ip_address'),
            storage_cluster_ip_address=data.get('storage_cluster_ip_address'),
            storage_public_ip_address=data.get('storage_public_ip_address'),
            status=s_fields.NodeStatus.CREATING)
        node.create()

        self.executor.submit(self._node_create, ctxt, node, data)
        return node
