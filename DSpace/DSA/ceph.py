#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time

from DSpace import exception
from DSpace.DSA.base import AgentBaseHandler
from DSpace.objects import fields as s_fields
from DSpace.taskflows.node import NodeTask
from DSpace.tools.ceph import CephTool
from DSpace.tools.disk import DiskTool as DiskTool
from DSpace.tools.file import File as FileTool
from DSpace.tools.package import Package as PackageTool
from DSpace.tools.service import Service as ServiceTool

logger = logging.getLogger(__name__)


class CephHandler(AgentBaseHandler):
    def ceph_conf_write(self, context, content):
        logger.debug("Write Ceph Conf")
        client = self._get_executor()
        file_tool = FileTool(client)
        file_tool.mkdir("/etc/ceph")
        file_tool.write("/etc/ceph/ceph.conf", content)
        return True

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
        ceph_tool.disk_clear_partition_table(osd.disk.name)
        ceph_tool.disk_prepare(**kwargs)
        return True

    def ceph_active_disk(self, context, osd):
        client = self._get_ssh_executor()
        ceph_tool = CephTool(client)
        ceph_tool.disk_active(osd.disk.name)

    def ceph_osd_package_install(self, context):
        logger.info('install ceph-osd package')
        client = self._get_ssh_executor()
        # install package
        package_tool = PackageTool(client)
        package_tool.install(["ceph-osd", "dspace-disk"])
        # create osd dir
        file_tool = FileTool(client)
        file_tool.mkdir("/var/lib/ceph/osd/")
        return True

    def ceph_osd_package_uninstall(self, context):
        logger.info('uninstall ceph-osd package')
        client = self._get_ssh_executor()
        # install package
        package_tool = PackageTool(client)
        package_tool.uninstall(["ceph-osd", "dspace-disk"])
        # remove osd dir
        file_tool = FileTool(client)
        file_tool.rm("/var/lib/ceph/osd/")
        return True

    def ceph_package_uninstall(self, context):
        logger.info('uninstall ceph-common package')
        client = self._get_ssh_executor()
        # install package
        package_tool = PackageTool(client)
        package_tool.uninstall(["ceph-common", "libcephfs2"])
        file_tool = FileTool(client)
        file_tool.rm("/var/lib/ceph/")
        file_tool.rm("/etc/ceph/")
        return True

    def ceph_osd_create(self, context, osd):
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

    def _data_clear(self, client, partition_name):
        disk_tool = DiskTool(client)
        disk_tool.data_clear(partition_name)
        logger.info("clear partition %s success", partition_name)

    def ceph_osd_destroy(self, context, osd):
        logger.info("osd %s(osd.%s), destroy", osd.id, osd.osd_id)
        client = self._get_ssh_executor()
        ceph_tool = CephTool(client)
        # mark oud
        ceph_tool.osd_mark_out(osd.osd_id)
        logger.info("osd %s(osd.%s), mark out", osd.id, osd.osd_id)
        # stop
        ceph_tool.osd_stop(osd.osd_id)
        logger.info("osd %s(osd.%s), service stop", osd.id, osd.osd_id)
        # unmount
        ceph_tool.osd_umount(osd.osd_id)
        logger.info("osd %s(osd.%s), umount", osd.id, osd.osd_id)
        # clean data
        ceph_tool.osd_zap(osd.disk.name)
        logger.info("osd %s(osd.%s), zap success", osd.id, osd.osd_id)
        ceph_tool.osd_remove_from_cluster(osd.osd_id)
        logger.info("osd %s(osd.%s), remove success", osd.id, osd.osd_id)
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
                    service_tool.status(mgr_service) == "active" and
                    service_tool.status(mds_service) == "active"):
                break
            logger.info("Mon not start success, retry after 1 second")
            retry_times += 1
            time.sleep(1)
        logger.debug("mon is ready")

    def ceph_mon_create(self, context, fsid, ceph_auth='none'):
        client = self._get_ssh_executor()
        # install package
        package_tool = PackageTool(client)
        package_tool.install(["ceph-mon", "ceph-mgr", "ceph-mds"])
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
                              ceph_auth=ceph_auth)

        # enable and start ceph-mon
        service_tool = ServiceTool(client)
        service_tool.enable("ceph-mon@{}".format(self.node.hostname))
        service_tool.start("ceph-mon@{}".format(self.node.hostname))
        service_tool.enable("ceph-mgr@{}".format(self.node.hostname))
        service_tool.start("ceph-mgr@{}".format(self.node.hostname))
        service_tool.enable("ceph-mds@{}".format(self.node.hostname))
        service_tool.start("ceph-mds@{}".format(self.node.hostname))

        self._wait_mon_ready(client)
        ceph_tool.module_enable("prometheus")
        return True

    def ceph_mon_remove(self, context, last_mon=False):
        client = self._get_ssh_executor()
        # remove mon service
        ceph_tool = CephTool(client)
        if not last_mon:
            ceph_tool.mon_uninstall(self.node.hostname)
        # stop and disable ceph-mon
        service_tool = ServiceTool(client)
        service_tool.stop("ceph-mon@{}".format(self.node.hostname))
        service_tool.disable("ceph-mon@{}".format(self.node.hostname))
        service_tool.stop("ceph-mgr@{}".format(self.node.hostname))
        service_tool.disable("ceph-mgr@{}".format(self.node.hostname))
        service_tool.stop("ceph-mds@{}".format(self.node.hostname))
        service_tool.disable("ceph-mds@{}".format(self.node.hostname))

        package_tool = PackageTool(client)
        package_tool.uninstall(["ceph-mon", "ceph-mgr", "ceph-mds"])
        # remove mon and dir
        file_tool = FileTool(client)
        file_tool.rm("/var/lib/ceph/mon/ceph-{}".format(self.node.hostname))
        file_tool.rm("/var/lib/ceph/mgr/ceph-{}".format(self.node.hostname))
        file_tool.rm("/var/lib/ceph/mds/ceph-{}".format(self.node.hostname))
        return True

    def osd_create(self, ctxt, node, osd):
        task = NodeTask(ctxt, node)
        task.ceph_osd_install(osd)

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
        package_tool.install(["ceph-radosgw"])
        return True

    def ceph_rgw_package_uninstall(self, context):
        logger.info('Uninstall ceph-radosgw package')
        client = self._get_ssh_executor()
        # Uninstall package
        package_tool = PackageTool(client)
        package_tool.uninstall(["ceph-radosgw"])
        return True

    def _check_radosgw_status(self, client, radosgw):
        logger.info("Check radosgw service status")
        service_tool = ServiceTool(client)
        status = service_tool.status(
            "ceph-radosgw@rgw.{}".format(radosgw.name))
        if status != s_fields.ServiceStatus.ACTIVE:
            raise exception.StorException(
                message='Radosgw service start failed')

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
