from oslo_log import log as logging

from DSpace import exception
from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.i18n import _
from DSpace.objects.fields import AllActionType as Action
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.objects.fields import ConfigKey
from DSpace.taskflows.node import NodeTask

logger = logging.getLogger(__name__)


class SysConfigHandler(AdminBaseHandler):
    def sysconf_get_all(self, ctxt):
        result = {}
        sysconfs = objects.SysConfigList.get_all(
            ctxt, filters={"cluster_id": ctxt.cluster_id})
        keys = ['cluster_name', 'admin_cidr', 'public_cidr', 'cluster_cidr',
                'gateway_cidr', 'chrony_server', 'ceph_version',
                'ceph_version_name', 'is_admin', 'is_import', 'import_task_id']
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
                self.finish_action(begin_action, None, 'sysconfig',
                                   chrony_server, status)
            except exception.StorException as e:
                logger.error("update chrony server error: %s", e)
                status = 'fail'
                self.finish_action(begin_action, None, 'sysconfig',
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

    def update_sysinfo(self, ctxt, sysinfos):
        gateway_cidr = sysinfos.get('gateway_cidr')
        if gateway_cidr:
            self._gateway_check(ctxt)
        cluster_cidr = sysinfos.get('cluster_cidr')
        public_cidr = sysinfos.get('public_cidr')
        admin_cidr = sysinfos.get('admin_cidr')
        chrony_server = sysinfos.get('chrony_server')
        cluster_name = sysinfos.get('cluster_name')
        if chrony_server:
            old_chrony = objects.sysconfig.sys_config_get(
                ctxt, "chrony_server")
            if old_chrony != chrony_server:
                logger.info("trying to update chrony_server from %s to %s",
                            old_chrony, chrony_server)
                begin_action = self.begin_action(ctxt, Resource.SYSCONFIG,
                                                 Action.UPDATE_CLOCK_SERVER)
                objects.sysconfig.sys_config_set(ctxt, 'chrony_server',
                                                 chrony_server)
                self.task_submit(self._update_chrony, ctxt, chrony_server,
                                 begin_action)
        if cluster_name:
            objects.sysconfig.sys_config_set(ctxt, 'cluster_name',
                                             cluster_name)
        if admin_cidr:
            objects.sysconfig.sys_config_set(ctxt, 'admin_cidr',
                                             admin_cidr)
        if public_cidr:
            objects.sysconfig.sys_config_set(ctxt, 'public_cidr',
                                             public_cidr)
        if cluster_cidr:
            objects.sysconfig.sys_config_set(ctxt, 'cluster_cidr',
                                             cluster_cidr)
        if gateway_cidr:
            begin_action = self.begin_action(ctxt, Resource.SYSCONFIG,
                                             Action.UPDATE_CLOCK_SERVER)
            objects.sysconfig.sys_config_set(ctxt, 'gateway_cidr',
                                             cluster_cidr)
            self.finish_action(begin_action, None, 'sysconfig', gateway_cidr)

    def image_namespace_get(self, ctxt):
        image_namespace = objects.sysconfig.sys_config_get(
            ctxt, ConfigKey.IMAGE_NAMESPACE)
        return image_namespace
