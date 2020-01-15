#!/usr/bin/env python
# -*- coding: utf-8 -*-
import configparser
import logging
import time
from io import StringIO
from os import path

import six

from DSpace import exception
from DSpace.DSA.base import AgentBaseHandler
from DSpace.tools.ceph import CephTool
from DSpace.tools.ceph_config import CephConfigTool
from DSpace.tools.disk import DiskTool as DiskTool
from DSpace.tools.file import File as FileTool
from DSpace.tools.package import Package as PackageTool
from DSpace.tools.service import Service as ServiceTool
from DSpace.tools.system import System as SystemTool
from DSpace.utils import retry
from DSpace.utils.cluster_config import CEPH_CONFIG_PATH
from DSpace.utils.coordination import synchronized

logger = logging.getLogger(__name__)


class CephHandler(AgentBaseHandler):
    @synchronized("ceph-config-{self.node.id}")
    def ceph_conf_write(self, context, content):
        logger.debug("Write Ceph Conf")
        client = self._get_executor()
        file_tool = FileTool(client)
        file_tool.mkdir("/etc/ceph")
        file_tool.write("/etc/ceph/ceph.conf", content)
        return True

    def ceph_key_write(self, context, entity, keyring_dir, keyring_name,
                       content):
        logger.info("write ceph keys to %s/%s", keyring_dir, keyring_name)
        configer = configparser.ConfigParser()
        configer[entity] = {}
        configer[entity]["key"] = content
        buf = StringIO()
        configer.write(buf)
        client = self._get_executor()
        file_tool = FileTool(client)
        file_tool.mkdir(keyring_dir)
        keyring_path = path.join(keyring_dir, keyring_name)
        file_tool.write(keyring_path, buf.getvalue())
        file_tool.chown(keyring_path, user='ceph', group='ceph')
        file_tool.chmod(keyring_path, "0644")

    @synchronized("ceph-osd-{osd.id}")
    def ceph_prepare_disk(self, context, osd):
        kwargs = {
            "diskname": osd.disk.name,
            "backend": osd.type,
        }
        if osd.fsid and osd.osd_id:
            kwargs['fsid'] = osd.fsid
            kwargs['osd_id'] = osd.osd_id
        if osd.cache_partition_id:
            kwargs['cache_partition'] = osd.cache_partition.name
        if osd.db_partition_id:
            kwargs['db_partition'] = osd.db_partition.name
        if osd.wal_partition_id:
            kwargs['wal_partition'] = osd.wal_partition.name
        if osd.journal_partition_id:
            kwargs['journal_partition'] = osd.journal_partition.name

        client = self._get_ssh_executor()
        ceph_tool = CephTool(client)
        self._data_clear(client, osd.disk.name)
        ceph_tool.osd_zap(osd.disk.name)
        ceph_tool.disk_prepare(**kwargs)
        return True

    @synchronized("ceph-config-{osd.id}")
    def ceph_active_disk(self, context, osd):
        client = self._get_ssh_executor()
        ceph_tool = CephTool(client)
        ceph_tool.disk_active(osd.disk.name)

    def ceph_osd_package_install(self, context):
        logger.info('install ceph-osd package')
        client = self._get_ssh_executor()
        # install package
        package_tool = PackageTool(client)
        package_tool.install(["ceph-osd"])
        # create osd dir
        file_tool = FileTool(client)
        file_tool.mkdir("/var/lib/ceph/osd/")
        return True

    def ceph_osd_package_uninstall(self, context):
        logger.info('uninstall ceph-osd package')
        client = self._get_ssh_executor()
        # uninstall package
        package_tool = PackageTool(client)
        osd_packages = ['ceph-osd']
        package_tool.uninstall_nodeps(osd_packages)
        # remove osd dir
        file_tool = FileTool(client)
        file_tool.rm("/var/lib/ceph/osd/")
        return True

    def ceph_package_uninstall(self, context):
        logger.info('uninstall ceph package')
        client = self._get_ssh_executor()
        # uninstall package
        package_tool = PackageTool(client)
        ceph_packages = [
            'ceph-resource-agents', 'ceph', 'ceph-base', 'ceph-common',
            'ceph-selinux', 'ceph-mds', 'ceph-mon', 'ceph-osd', 'ceph-mgr',
            'libcephfs2', 'libcephfs-devel', 'python-cephfs',
            'libcephfs_jni1-devel', 'libcephfs_jni1', 'cephfs-java',
            'librbd1', 'librbd-devel', 'python-rbd', 'rbd-fuse',
            'rbd-nbd', 'rbd-mirror', 'librgw2', 'librgw-devel',
            'python-rgw', 'librados2', 'librados-devel',
            'libradosstriper1-devel', 'python-rados', 'libradosstriper1',
            'ceph-radosgw', 'python-ceph-compat', 'ceph-fuse',
            'ceph-libs-compat', 'ceph-devel-compat'
        ]
        package_tool.uninstall_nodeps(ceph_packages)
        file_tool = FileTool(client)
        file_tool.rm("/var/lib/ceph/")
        file_tool.rm("/etc/ceph/")
        return True

    @retry(exception.DeviceOrResourceBusy)
    def _clean_osd(self, context, osd):
        logger.info("osd %s(osd.%s), clean", osd.id, osd.osd_id)
        client = self._get_ssh_executor()
        ceph_tool = CephTool(client)
        # deactivate
        ceph_tool.osd_deactivate_flag(osd.osd_id)
        logger.info("osd %s(osd.%s), set deactivate flag", osd.id, osd.osd_id)
        # stop
        ceph_tool.osd_stop(osd.osd_id)
        logger.info("osd %s(osd.%s), service stop", osd.id, osd.osd_id)
        # unmount
        ceph_tool.osd_umount(osd.osd_id)
        logger.info("osd %s(osd.%s), umount", osd.id, osd.osd_id)
        # clear osd data
        ceph_tool.osd_zap(osd.disk.name)
        logger.info("osd %s(osd.%s), zap success", osd.id, osd.osd_id)
        if osd.cache_partition_id:
            self._data_clear(client, osd.cache_partition.name)
        if osd.db_partition_id:
            self._data_clear(client, osd.db_partition.name)
        if osd.wal_partition_id:
            self._data_clear(client, osd.wal_partition.name)
        if osd.journal_partition_id:
            self._data_clear(client, osd.journal_partition.name)
        logger.info("osd %s(osd.%s), clear partition success",
                    osd.id, osd.osd_id)

    @synchronized("ceph-osd-{osd.id}")
    def ceph_osd_clean(self, context, osd):
        self._clean_osd(context, osd)
        return osd

    def ceph_osd_offline(self, context, osd, umount):
        logger.info("osd %s(osd.%s), offline", osd.id, osd.osd_id)
        client = self._get_ssh_executor()
        ceph_tool = CephTool(client)
        ceph_tool.osd_stop(osd.osd_id)
        if umount:
            ceph_tool.osd_umount(osd.osd_id)

    @synchronized("ceph-osd-{osd.id}")
    def ceph_osd_restart(self, context, osd):
        logger.info("osd %s(osd.%s), restart", osd.id, osd.osd_id)
        client = self._get_ssh_executor()
        service_tool = ServiceTool(client)
        osd_service = "ceph-osd@{}".format(osd.osd_id)
        service_tool.restart(osd_service)

    @synchronized("ceph-config-{self.node.id}")
    def ceph_config_set(self, context, configs):
        logger.info("ceph config set: %s", configs)
        client = self._get_executor()
        config_tool = CephConfigTool(CEPH_CONFIG_PATH, client)
        for group, key_values in six.iteritems(configs):
            for key, value in six.iteritems(key_values):
                config_tool.set_value(key, value, group)
        config_tool.save()

    @synchronized("ceph-config-{self.node.id}")
    def ceph_config_clear_group(self, context, group):
        logger.info("ceph config clear: %s", group)
        client = self._get_executor()
        config_tool = CephConfigTool(CEPH_CONFIG_PATH, client)
        config_tool.clear_section(group)
        config_tool.save()

    def ceph_osd_create(self, context, osd, configs):
        logger.info("ceph osd create: %s, disk(%s)",
                    osd.osd_name, osd.disk.name)
        self.ceph_config_set(context, configs)
        self.ceph_prepare_disk(context, osd)
        self.ceph_active_disk(context, osd)
        return osd

    def ceph_services_restart(self, ctxt, types, service):
        client = self._get_ssh_executor()
        ceph_tool = CephTool(client)
        res = ceph_tool.systemctl_restart(types, service)
        return res

    def ceph_services_start(self, ctxt, types, service):
        client = self._get_ssh_executor()
        ceph_tool = CephTool(client)
        res = ceph_tool.systemctl_restart(types, service)
        return res

    def ceph_services_stop(self, ctxt, types, service):
        client = self._get_ssh_executor()
        ceph_tool = CephTool(client)
        res = ceph_tool.service_stop(types, service)
        return res

    def ceph_slow_request(self, ctxt, osds):
        client = self._get_ssh_executor()
        ceph_tool = CephTool(client)
        res = ceph_tool.slow_request_get(osds)
        return res

    def _data_clear(self, client, partition_name):
        disk_tool = DiskTool(client)
        disk_tool.data_clear(partition_name)
        logger.info("clear partition %s success", partition_name)

    @synchronized("ceph-osd-{osd.id}")
    def ceph_osd_destroy(self, context, osd):
        logger.info("osd %s(osd.%s), destroy", osd.id, osd.osd_id)
        client = self._get_ssh_executor()
        # clean osd
        self._clean_osd(context, osd)
        logger.info("osd %s(osd.%s), service disable", osd.id, osd.osd_id)
        service_tool = ServiceTool(client)
        osd_service = "ceph-osd@{}".format(osd.osd_id)
        service_tool.disable(osd_service)
        # clear config
        logger.info("osd %s(osd.%s), config clear", osd.id, osd.osd_id)
        self.ceph_config_clear_group(context, osd.osd_name)
        logger.info("osd %s(osd.%s), remove success", osd.id, osd.osd_id)
        return osd

    def _wait_mon_ready(self, client):
        logger.debug("wait monitor ready to work")
        retry_times = 0
        while True:
            if retry_times == 30:
                logger.exception("Monitor start failed")
                raise exception.InvalidInput("Monitor start failed")
            ceph_tool = CephTool(client)
            service_tool = ServiceTool(client)
            mgr_service = "ceph-mgr@{}".format(self.node.hostname)
            mds_service = "ceph-mds@{}".format(self.node.hostname)
            if (ceph_tool.check_mon_is_joined(str(self.node.public_ip)) and
                    service_tool.status(mgr_service) and
                    service_tool.status(mds_service)):
                break
            logger.info("Mon not start success, retry after 1 second")
            retry_times += 1
            time.sleep(1)
        logger.debug("mon is ready")

    def collect_keyring(self, context, entity):
        client = self._get_ssh_executor()
        ceph_tool = CephTool(client)
        return ceph_tool.collect_keyring(entity)

    def ceph_mon_pre_check(self, context, mon_data_avail_min=30):
        client = self._get_executor()
        sys_tool = SystemTool(client)
        fs_stat = sys_tool.get_node_fsstat()
        byte_total = (float)(fs_stat.f_blocks * fs_stat.f_bsize)
        byte_avail = (float)(fs_stat.f_bavail * fs_stat.f_bsize)
        if ((byte_avail/byte_total)*100) < mon_data_avail_min:
            raise exception.NodeLowSpaceException(
                percent=mon_data_avail_min)
        return True

    def ceph_mon_create(self, context, fsid, mon_secret=None,
                        mgr_dspace_port=None):
        client = self._get_ssh_executor()
        # install package
        package_tool = PackageTool(client)
        package_tool.install(["ceph-mon", "ceph-mgr", "ceph-mds",
                              "ceph-mgr-dspace"])
        # create mon dir
        file_tool = FileTool(client)
        file_tool.mkdir("/var/lib/ceph/mon/ceph-{}".format(self.node.hostname))
        file_tool.rm("/var/lib/ceph/mon/ceph-{}/*".format(self.node.hostname))
        file_tool.chown("/var/lib/ceph/mon", user='ceph', group='ceph')
        # create mgr dir
        file_tool.mkdir("/var/lib/ceph/mgr/ceph-{}".format(self.node.hostname))
        file_tool.rm("/var/lib/ceph/mgr/ceph-{}/*".format(self.node.hostname))
        file_tool.chown("/var/lib/ceph/mgr", user='ceph', group='ceph')
        # create mds dir
        file_tool.mkdir("/var/lib/ceph/mds/ceph-{}".format(self.node.hostname))
        file_tool.rm("/var/lib/ceph/mds/ceph-{}/*".format(self.node.hostname))
        file_tool.chown("/var/lib/ceph/mds", user='ceph', group='ceph')

        ceph_tool = CephTool(client)
        ceph_tool.mon_install(self.node.hostname,
                              fsid,
                              mon_secret=mon_secret)
        # enable and start ceph-mon
        service_tool = ServiceTool(client)
        service_tool.enable("ceph-mon@{}".format(self.node.hostname))
        service_tool.start("ceph-mon@{}".format(self.node.hostname))

        # Generate client.admin key and bootstrap key when mon is ready
        # If not enable cephx, these keys exists but no effect
        if mon_secret:
            file_tool.rm("/etc/ceph/*.keyring")
            file_tool.rm("/var/lib/ceph/bootstrap-*/*")
            ceph_tool.create_keys(self.node.hostname)
            ceph_tool.create_mgr_keyring(self.node.hostname)
            ceph_tool.create_mds_keyring(self.node.hostname)

        service_tool.enable("ceph-mgr@{}".format(self.node.hostname))
        service_tool.start("ceph-mgr@{}".format(self.node.hostname))
        service_tool.enable("ceph-mds@{}".format(self.node.hostname))
        service_tool.start("ceph-mds@{}".format(self.node.hostname))

        self._wait_mon_ready(client)

        public_ip = self.node.public_ip

        ceph_tool.module_enable("dspace", str(public_ip), mgr_dspace_port)
        return True

    def ceph_mon_remove(self, context):
        client = self._get_ssh_executor()
        # remove mon service
        ceph_tool = CephTool(client)
        sys_tool = SystemTool(client)
        if sys_tool.check_package('ceph-mon'):
            ceph_tool.mon_remove(self.node.hostname)
        # stop and disable ceph-mon
        service_tool = ServiceTool(client)
        service_tool.stop("ceph-mon@{}".format(self.node.hostname))
        service_tool.disable("ceph-mon@{}".format(self.node.hostname))
        service_tool.stop("ceph-mgr@{}".format(self.node.hostname))
        service_tool.disable("ceph-mgr@{}".format(self.node.hostname))
        service_tool.stop("ceph-mds@{}".format(self.node.hostname))
        service_tool.disable("ceph-mds@{}".format(self.node.hostname))

        # uninstall package
        package_tool = PackageTool(client)
        mon_packages = ['ceph-mon', 'ceph-mgr', 'ceph-mgr-dspace', 'ceph-mds']
        package_tool.uninstall_nodeps(mon_packages)
        # remove mon and dir
        file_tool = FileTool(client)
        file_tool.rm("/var/lib/ceph/mon/ceph-{}".format(self.node.hostname))
        file_tool.rm("/var/lib/ceph/mgr/ceph-{}".format(self.node.hostname))
        file_tool.rm("/var/lib/ceph/mds/ceph-{}".format(self.node.hostname))
        return True

    def ceph_config_update(self, ctxt, values):
        logger.debug('Update ceph config for this node')
        client = self._get_executor()
        ceph_tool = CephTool(client)
        try:
            ceph_tool.ceph_config_update(values)
        except exception.StorException as e:
            logger.error('Update ceph config error: {}'.format(e))
            return False
        return True

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

    def ceph_rgw_create(self, ctxt, radosgw, zone_params):
        logger.info('Create ceph-radosgw service')
        client = self._get_ssh_executor()

        # Set zone params
        file_tool = FileTool(client)
        zone_file_path = "/etc/ceph/radosgw_zone.json"
        file_tool.write(zone_file_path, zone_params)
        ceph_tool = CephTool(client)
        ceph_tool.radosgw_admin_zone_set(zone_params, zone_file_path)

        # Enable and start ceph-radosgw service
        service_tool = ServiceTool(client)
        service_tool.enable("ceph-radosgw@rgw.{}".format(radosgw.name))
        service_tool.start("ceph-radosgw@rgw.{}".format(radosgw.name))

        self._check_radosgw_status(client, radosgw)
        return radosgw

    def ceph_rgw_destroy(self, ctxt, radosgw):
        logger.info('Destroy ceph-radosgw service')
        client = self._get_ssh_executor()

        # Stop and disable ceph-radosgw service
        service_tool = ServiceTool(client)
        service_tool.stop("ceph-radosgw@rgw.{}".format(radosgw.name))
        service_tool.disable("ceph-radosgw@rgw.{}".format(radosgw.name))

        logger.info("Radosgw %s destroy success",
                    radosgw.name)
        return radosgw

    def get_osds_status(self, ctxt, osds):
        logger.debug("Check osd status")
        ssh_client = self._get_ssh_executor()
        service_tool = ServiceTool(ssh_client)
        osd_status = {}
        for osd in osds:
            try:
                name = "ceph-osd@{}".format(osd.osd_id)
                if service_tool.status(name=name):
                    status = "up"
                else:
                    status = "down"
            except exception.StorException as e:
                logger.error("Get service status error: {}".format(e))
                status = "down"
            osd_status.update({
                "osd.{}".format(osd.osd_id): status
            })
        return osd_status
