#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from DSpace import exception
from DSpace.DSA.base import AgentBaseHandler
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
        client = self._get_ssh_executor()
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
        package_tool.uninstall(["ceph-common"])
        return True

    def ceph_osd_create(self, context, osd):
        self.ceph_prepare_disk(context, osd)
        self.ceph_active_disk(context, osd)
        return osd

    def _data_clear(self, client, partition_name):
        disk_tool = DiskTool(client)
        try:
            disk_tool.data_clear(partition_name)
        except Exception as e:
            logger.exception(e)

    def ceph_osd_destroy(self, context, osd):
        client = self._get_ssh_executor()
        ceph_tool = CephTool(client)
        try:
            ceph_tool.osd_deactivate(osd.disk.name)
        except Exception as e:
            logger.exception(e)
        try:
            ceph_tool.osd_zap(osd.disk.name)
        except Exception as e:
            logger.exception(e)
        if osd.cache_partition_id:
            self._data_clear(client, osd.cache_partition.name)
        if osd.db_partition_id:
            self._data_clear(client, osd.db_partition.name)
        if osd.wal_partition_id:
            self._data_clear(client, osd.wal_partition.name)
        if osd.journal_partition_id:
            self._data_clear(client, osd.journal_partition.name)
        return osd

    def ceph_mon_create(self, context, ceph_auth='none'):
        client = self._get_ssh_executor()
        # install package
        package_tool = PackageTool(client)
        package_tool.install(["ceph-mon", "ceph-mgr"])
        # create mon dir
        file_tool = FileTool(client)
        file_tool.mkdir("/var/lib/ceph/mon/ceph-{}".format(self.node.hostname))
        file_tool.chown("/var/lib/ceph/mon", user='ceph', group='ceph')
        # create mgr dir
        file_tool.mkdir("/var/lib/ceph/mgr/ceph-{}".format(self.node.hostname))
        file_tool.chown("/var/lib/ceph/mgr", user='ceph', group='ceph')

        ceph_tool = CephTool(client)
        ceph_tool.mon_install(self.node.hostname,
                              self.node.cluster_id,
                              ceph_auth=ceph_auth)

        # enable and start ceph-mon
        service_tool = ServiceTool(client)
        service_tool.enable("ceph-mon@{}".format(self.node.hostname))
        service_tool.start("ceph-mon@{}".format(self.node.hostname))
        service_tool.enable("ceph-mgr@{}".format(self.node.hostname))
        service_tool.start("ceph-mgr@{}".format(self.node.hostname))
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

        # remove mon and dir
        file_tool = FileTool(client)
        file_tool.rm("/var/lib/ceph/mon/ceph-{}".format(self.node.hostname))
        file_tool.rm("/var/lib/ceph/mgr/ceph-{}".format(self.node.hostname))
        return True

    def osd_create(self, ctxt, node, osd):
        task = NodeTask(ctxt, node)
        task.ceph_osd_install(osd)

    def ceph_config_update(self, ctxt, values):
        logger.debug('Update ceph config for this node')
        node_task = NodeTask(ctxt, node=None)
        try:
            node_task.ceph_config_update(values)
        except exception.StorException as e:
            logger.error('Update ceph config error: {}'.format(e))
            return False
        return True
