#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from t2stor import exception
from t2stor.agent.base import AgentBaseHandler
from t2stor.taskflows.node import NodeTask
from t2stor.tools.ceph import CephTool
from t2stor.tools.disk import DiskTool as DiskTool
from t2stor.tools.file import File as FileTool
from t2stor.tools.package import Package as PackageTool
from t2stor.tools.service import Service as ServiceTool

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

    def ceph_osd_create(self, context, osd):
        self.ceph_prepare_disk(context, osd)
        self.ceph_active_disk(context, osd)
        return osd

    def ceph_osd_destroy(self, context, osd):
        client = self._get_ssh_executor()
        ceph_tool = CephTool(client)
        ceph_tool.osd_deactivate(osd.disk.name)
        ceph_tool.osd_zap(osd.disk.name)
        disk_tool = DiskTool(client)
        if osd.cache_partition_id:
            disk_tool.data_clear(osd.cache_partition.name)
        if osd.db_partition_id:
            disk_tool.data_clear(osd.db_partition.name)
        if osd.wal_partition_id:
            disk_tool.data_clear(osd.wal_partition.name)
        if osd.journal_partition_id:
            disk_tool.data_clear(osd.journal_partition.name)
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
