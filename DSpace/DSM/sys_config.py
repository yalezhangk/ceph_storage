from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.objects import fields as s_fields

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

    def update_chrony(self, ctxt, chrony_server):
        sysconf = objects.SysConfig(
            ctxt, key="chrony_server", value=chrony_server,
            value_type=s_fields.ConfigType.STRING)
        sysconf.create()

    def update_sysinfo(self, ctxt, cluster_name, admin_cidr, public_cidr,
                       cluster_cidr, gateway_cidr):
        # TODO check a object exists
        sysconf = None
        if cluster_name:
            sysconf = objects.SysConfig(
                ctxt, key="cluster_name", value=cluster_name,
                value_type=s_fields.ConfigType.STRING)
            sysconf.create()
        if admin_cidr:
            sysconf = objects.SysConfig(
                ctxt, key="admin_cidr", value=admin_cidr,
                value_type=s_fields.ConfigType.STRING)
            sysconf.create()
        if public_cidr:
            sysconf = objects.SysConfig(
                ctxt, key="public_cidr", value=public_cidr,
                value_type=s_fields.ConfigType.STRING)
            sysconf.create()
        if cluster_cidr:
            sysconf = objects.SysConfig(
                ctxt, key="cluster_cidr", value=cluster_cidr,
                value_type=s_fields.ConfigType.STRING)
            sysconf.create()
        if gateway_cidr:
            sysconf = objects.SysConfig(
                ctxt, key="gateway_cidr", value=gateway_cidr,
                value_type=s_fields.ConfigType.STRING)
            sysconf.create()

    def image_namespace_get(self, ctxt):
        image_namespace = objects.sysconfig.sys_config_get(ctxt,
                                                           "image_namespace")
        return image_namespace
