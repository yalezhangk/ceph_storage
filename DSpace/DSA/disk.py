#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

import six

from DSpace import exception
from DSpace.DSA.base import AgentBaseHandler
from DSpace.objects import fields as s_fields
from DSpace.tools.disk import DiskTool
from DSpace.tools.pysmart import Device as DeviceTool
from DSpace.tools.storcli import StorCli as StorCliTool

logger = logging.getLogger(__name__)


class DiskHandler(AgentBaseHandler):
    def disk_smart_get(self, ctxt, node, name):
        ssh_client = self._get_ssh_executor(node)
        if not ssh_client:
            return []

        disk_name = '/dev/' + name
        device_tool = DeviceTool(name=disk_name, ssh=ssh_client)
        smart = device_tool.all_attributes()
        if not smart:
            return []
        return smart

    def disk_light(self, ctxt, led, node, name):
        logger.debug("Disk Light: %s", name)
        ssh_client = self._get_ssh_executor(node)
        if not ssh_client:
            return False

        disk_name = '/dev/' + name
        storcli = StorCliTool(ssh=ssh_client, disk_name=disk_name)
        action = 'start' if led == 'on' else 'stop'
        return storcli.disk_light(action)

    def _get_disk_partition_steps(self, num, role, name, disk_size):
        i = 1
        partitions = []
        if role == s_fields.DiskPartitionRole.CACHE:
            if (disk_size / num) >= s_fields.OsdDBLevel.LARGE * 2:
                db_size = s_fields.OsdDBLevel.LARGE
            elif (disk_size / num) >= s_fields.OsdDBLevel.MEDIUM * 2:
                db_size = s_fields.OsdDBLevel.MEDIUM
            else:
                db_size = s_fields.OsdDBLevel.SMALL
            cache_size = (disk_size / num) - db_size

            while i <= num:
                partitions.append({
                    "name": name + str(i),
                    "size": cache_size,
                    "role": "cache",
                })
                i += 1
            while i <= num * 2:
                partitions.append({
                    "name": name + str(i),
                    "size": db_size,
                    "role": "db",
                })
                i += 1
        else:
            partition_size = disk_size / num
            while i <= num:
                partitions.append({
                    "name": name + str(i),
                    "size": partition_size,
                    "role": role,
                })
                i += 1
        return partitions

    def disk_partitions_create(self, ctxt, node, disk, values):
        logger.info('Make accelerate disk partition: %s', disk.name)
        executor = self._get_ssh_executor()
        disk_tool = DiskTool(executor)
        partition_num = values.get('partition_num')
        partition_role = values.get('partition_role')
        partitions = self._get_disk_partition_steps(
            partition_num, partition_role, disk.name, disk.size)
        try:
            disk_tool.partitions_clear(disk.name)
            disk_tool.partitions_create(disk.name, partitions)
            guid = disk_tool.get_disk_guid(disk.name)
            logger.debug("Partitions: {}".format(partitions))
            if partition_role == s_fields.DiskPartitionRole.CACHE:
                partitions = list(filter(
                    lambda x: x['role'] != s_fields.DiskPartitionRole.DB,
                    partitions))
            return guid, partitions
        except exception.StorException as e:
            logger.error("Create partitions error: {}".format(e))
            return None, []

    def disk_partitions_remove(self, ctxt, node, name):
        logger.debug('Remove cache disk partitions: %s', name)
        executor = self._get_executor()
        disk_tool = DiskTool(executor)
        try:
            _success = disk_tool.partitions_clear(name)
        except exception.StorException as e:
            logger.error("Create partitions error: {}".format(e))
            _success = False
        return _success

    def disk_get_all(self, ctxt, node):
        executor = self._get_executor()
        disk_tool = DiskTool(executor)
        disks = disk_tool.all()

        ssh_executor = self._get_ssh_executor()
        disk_tool = DiskTool(ssh_executor)
        for name, data in six.iteritems(disks):
            disk_tool.update_udev_info(name, data)
        return disks
