import json
import logging

import six

from DSpace import exception
from DSpace.DSA.base import AgentBaseHandler
from DSpace.tools.ceph import CephTool
from DSpace.tools.file import File as FileTool
from DSpace.tools.package import Package as PackageTool
from DSpace.tools.radosgw_admin import RadosgwAdminCMD
from DSpace.tools.service import Service as ServiceTool

logger = logging.getLogger(__name__)


class RadosgwHandler(AgentBaseHandler):

    def ceph_rgw_package_install(self, context):
        logger.info('Install ceph-radosgw package')
        client = self._get_ssh_executor()
        # Install package
        package_tool = PackageTool(client)
        package_tool.install_rgw()
        return True

    def ceph_rgw_package_uninstall(self, context):
        logger.info('Uninstall ceph-radosgw package')
        client = self._get_ssh_executor()
        # Uninstall package
        package_tool = PackageTool(client)
        package_tool.uninstall_rgw()
        return True

    def _check_radosgw_status(self, client, radosgw):
        logger.info("Check radosgw service status")
        service_tool = ServiceTool(client)
        status = service_tool.status(
            "ceph-radosgw@rgw.{}".format(radosgw.name))
        if not status:
            raise exception.StorException(
                message='Radosgw service start failed')

    def create_rgw_keyring(self, ctxt, radosgw):
        client = self._get_ssh_executor()
        file_tool = FileTool(client)
        ceph_tool = CephTool(client)
        rgw_data_dir = "/var/lib/ceph/radosgw/ceph-rgw.{}".format(radosgw.name)
        file_tool.mkdir(rgw_data_dir)
        ceph_tool.create_rgw_keyring(radosgw.name, rgw_data_dir)
        file_tool.chown(rgw_data_dir, user='ceph', group='ceph')

    def ceph_rgw_create(self, ctxt, radosgw):
        logger.info('Create ceph-radosgw service: %s', radosgw.name)
        client = self._get_ssh_executor()
        # Enable and start ceph-radosgw service
        service_tool = ServiceTool(client)
        service_tool.enable("ceph-radosgw@rgw.{}".format(radosgw.name))
        service_tool.start("ceph-radosgw@rgw.{}".format(radosgw.name))

        self._check_radosgw_status(client, radosgw)
        return radosgw

    def ceph_rgw_destroy(self, ctxt, radosgw):
        logger.info('Destroy ceph-radosgw service: %s', radosgw.name)
        client = self._get_ssh_executor()

        # Stop and disable ceph-radosgw service
        service_tool = ServiceTool(client)
        service_tool.stop("ceph-radosgw@rgw.{}".format(radosgw.name))
        service_tool.disable("ceph-radosgw@rgw.{}".format(radosgw.name))

        logger.info("Radosgw %s destroy success",
                    radosgw.name)
        return radosgw

    def rgw_zone_init(self, ctxt, realm_name, zonegroup_name, zone_name,
                      zone_file_path, pool_sets):
        logger.info("Initialize zone: realm %s, zonegroup %s, zone %s",
                    realm_name, zonegroup_name, zone_name)
        ssh_client = self._get_ssh_executor()
        rgwcmd = RadosgwAdminCMD(ssh_client)
        realm = rgwcmd.realm_create(realm_name, default=True)
        zonegroup = rgwcmd.zonegourp_create(zonegroup_name, realm["name"],
                                            master=True, default=True)
        zone = rgwcmd.zone_create(zone_name, zonegroup["name"],
                                  realm["name"], master=True, default=True)
        for k, v in six.iteritems(pool_sets):
            zone[k] = v.format(zone["name"])

        file_tool = FileTool(ssh_client)
        file_tool.write(zone_file_path, json.dumps(zone))
        zone = rgwcmd.zone_set(zone["name"], zone_file_path)
        return zone

    def placement_remove(self, ctxt, name):
        logger.info("Remove placement %s", name)
        ssh_client = self._get_ssh_executor()
        rgwcmd = RadosgwAdminCMD(ssh_client)
        rgwcmd.placement_remove(name)

    def user_create_cmd(self, ctxt, name, display_name, access_key,
                        secret_key):
        logger.info("Create user: name %s, display_name %s, access_key %s, "
                    "secret_key %s", name, display_name, access_key, secret_key
                    )
        ssh_client = self._get_ssh_executor()
        rgwcmd = RadosgwAdminCMD(ssh_client)
        user = rgwcmd.user_create(
            name, display_name=display_name, access_key=access_key,
            secret_key=secret_key)
        return user

    def caps_add(self, ctxt, user, caps):
        logger.info("Add caps %s to user %s", caps, user)
        ssh_client = self._get_ssh_executor()
        rgwcmd = RadosgwAdminCMD(ssh_client)
        user = rgwcmd.caps_add(user, caps)
        return user

    def period_update(self, ctxt, commit=True):
        logger.info("Period update, commit=%s", commit)
        ssh_client = self._get_ssh_executor()
        rgwcmd = RadosgwAdminCMD(ssh_client)
        rgwcmd.period_update(commit=commit)

    def create_object_policy(self, ctxt, name, index_pool_name, data_pool_name,
                             compression):
        ssh_client = self._get_ssh_executor()
        rgw_cmd = RadosgwAdminCMD(ssh_client)
        rgw_cmd.placement_create(name, index_pool_name, data_pool_name,
                                 compression=compression)

    def set_default_object_policy(self, ctxt, name):
        rgw_cmd = RadosgwAdminCMD(self._get_ssh_executor())
        rgw_cmd.placement_set_default(name)

    def delete_object_policy(self, ctxt, name):
        rgw_cmd = RadosgwAdminCMD(self._get_ssh_executor())
        rgw_cmd.placement_remove(name)

    def modify_object_policy(self, ctxt, name, options):
        """
        :param options: {"index_type": 0, "compression": "zlib"}
        :return:
        """
        rgw_cmd = RadosgwAdminCMD(self._get_ssh_executor())
        rgw_cmd.placement_modify(name, options)
