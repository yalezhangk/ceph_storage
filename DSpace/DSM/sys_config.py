from netaddr import IPAddress
from netaddr import IPNetwork
from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.objects.fields import ConfigKey
from DSpace.objects.fields import ConfigType
from DSpace.taskflows.node import NodeTask

logger = logging.getLogger(__name__)
sys_config = _('sys_config')


class SysConfigHandler(AdminBaseHandler):
    def sysconf_get_all(self, ctxt):
        result = {}
        sysconfs = objects.SysConfigList.get_all(
            ctxt, filters={"cluster_id": ctxt.cluster_id})
        keys = ['cluster_name', 'admin_cidr', 'public_cidr', 'cluster_cidr',
                'gateway_cidr', 'chrony_server', 'ceph_version',
                'ceph_version_name', 'is_admin', 'is_import', 'import_task_id',
                'enable_cephx']
        for key in keys:
            result[key] = None
        for sysconf in sysconfs:
            if sysconf.key in keys:
                result[sysconf.key] = sysconf.value
        return result

    def _update_chrony(self, ctxt, chrony_server, begin_action=None):
        nodes = objects.NodeList.get_all(ctxt)
        if not nodes:
            logger.error("no nodes to update chrony server")
        for node in nodes:
            try:
                task = NodeTask(ctxt, node)
                # TODO Use ordered taskflow to execute update chrony
                self.task_submit(task.chrony_update())
                status = 'success'
                self.finish_action(begin_action, None, sys_config,
                                   chrony_server, status)
            except exception.StorException as e:
                logger.error("update chrony server error: %s", e)
                status = 'fail'
                self.finish_action(begin_action, None, sys_config,
                                   chrony_server, status, err_msg=str(e))
                raise e

    def _gateway_check(self, ctxt):
        nodes = objects.NodeList.get_all(
            ctxt, filters={"role_object_gateway": True})
        if nodes:
            raise exception.InvalidInput(
                _("Object gateway role has been set, "
                  "please remove all object gateway role")
            )

    def _enable_cephx_check(self, ctxt, enable_cephx):
        filters = {'role_monitor': True}
        mon_host = objects.NodeList.get_count(ctxt, filters=filters)
        old_cephx = objects.sysconfig.sys_config_get(
            ctxt, "enable_cephx", default=False)
        if mon_host and old_cephx != enable_cephx:
            raise exception.InvalidInput(
                _("Cluster has ceph-mon service, can't not "
                  "modify cephx dynamic"))

    def _update_gateway_ip(self, ctxt, gateway_cidr):
        logger.info("Update object_gateway_ip_address for nodes")
        nodes = objects.NodeList.get_all(ctxt)
        for node in nodes:
            if node.object_gateway_ip_address:
                if (IPAddress(node.object_gateway_ip_address) not in
                        IPNetwork(gateway_cidr)):
                    node.object_gateway_ip_address = None
                    node.save()
            nets = objects.NetworkList.get_all(
                ctxt, filters={"node_id": node.id})
            for net in nets:
                if not net.ip_address:
                    continue
                if IPAddress(net.ip_address) in IPNetwork(gateway_cidr):
                    node.object_gateway_ip_address = net.ip_address
                    node.save()
                    break

    def update_sysinfo(self, ctxt, sysinfos):
        gateway_cidr = sysinfos.get('gateway_cidr')
        enable_cephx = sysinfos.get('enable_cephx')
        if gateway_cidr:
            old_gateway = objects.sysconfig.sys_config_get(
                    ctxt, "gateway_cidr")
            if old_gateway != gateway_cidr:
                self._gateway_check(ctxt)
        if enable_cephx is not None:
            self._enable_cephx_check(ctxt, enable_cephx)
        cluster_cidr = sysinfos.get('cluster_cidr')
        public_cidr = sysinfos.get('public_cidr')
        admin_cidr = sysinfos.get('admin_cidr')
        chrony_server = sysinfos.get('chrony_server')
        cluster_name = sysinfos.get('cluster_name')
        if chrony_server:
            chrony_server = chrony_server.split(",")
            old_chrony = objects.sysconfig.sys_config_get(
                ctxt, "chrony_server")
            old_chrony = old_chrony.split(",")
            if sorted(old_chrony) != sorted(chrony_server):
                logger.info("trying to update chrony_server from %s to %s",
                            old_chrony, chrony_server)
                begin_action = self.begin_action(ctxt, Resource.SYSCONFIG,
                                                 Action.UPDATE_CLOCK_SERVER)
                objects.sysconfig.sys_config_set(ctxt, 'chrony_server',
                                                 ",".join(chrony_server))
                self.task_submit(self._update_chrony, ctxt, chrony_server,
                                 begin_action)
        if cluster_name:
            objects.sysconfig.sys_config_set(ctxt, 'cluster_name',
                                             cluster_name)
        if admin_cidr:
            old_admin = objects.sysconfig.sys_config_get(
                    ctxt, "admin_cidr")
            if old_admin != admin_cidr:
                objects.sysconfig.sys_config_set(ctxt, 'admin_cidr',
                                                 admin_cidr)
        if public_cidr:
            old_public = objects.sysconfig.sys_config_get(
                    ctxt, "public_cidr")
            if old_public != public_cidr:
                objects.sysconfig.sys_config_set(
                        ctxt, 'public_cidr', public_cidr)

        if cluster_cidr:
            objects.sysconfig.sys_config_set(ctxt, 'cluster_cidr',
                                             cluster_cidr)
        if enable_cephx is not None:
            objects.sysconfig.sys_config_set(
                ctxt, 'enable_cephx', enable_cephx,
                value_type=ConfigType.BOOL)
        if gateway_cidr:
            old_gateway_cidr = objects.sysconfig.sys_config_get(
                ctxt, "gateway_cidr")
            if old_gateway_cidr != gateway_cidr:
                begin_action = self.begin_action(ctxt, Resource.SYSCONFIG,
                                                 Action.UPDATE_GATEWAY_CIDR)
                objects.sysconfig.sys_config_set(ctxt, 'gateway_cidr',
                                                 gateway_cidr)
                self._update_gateway_ip(ctxt, gateway_cidr)
                self.finish_action(begin_action, None, sys_config,
                                   gateway_cidr)

    def image_namespace_get(self, ctxt):
        image_namespace = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.IMAGE_NAMESPACE)
        return image_namespace

    def package_ignore_get(self, ctxt):
        # True or False
        package_ignore = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.PACKAGE_IGNORE, default=False)
        return package_ignore

    def get_dsm_sysinfo(self, ctxt):
        # support DSA call
        image_namespace = self.image_namespace_get(ctxt)
        package_ignore = self.package_ignore_get(ctxt)
        return {
            'image_namespace': image_namespace,
            'package_ignore': package_ignore
        }

    def get_sysinfo(self, ctxt, keys):
        # support DSA call
        res = {}
        for key in keys:
            value = objects.sysconfig.sys_config_get(ctxt, key)
            res[key] = value
        return res
