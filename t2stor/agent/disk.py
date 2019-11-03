#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from t2stor import exception
from t2stor.agent.base import AgentBaseHandler
from t2stor.objects import fields as s_fields
from t2stor.tools.disk import DiskTool as DiskTool
from t2stor.tools.pysmart import Device as DeviceTool
from t2stor.tools.storcli import StorCli as StorCliTool

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
            return [
                {
                    "raw": "0",
                    "updated": "Always",
                    "num": "5",
                    "worst": "100",
                    "name": "Reallocated_Sector_Ct",
                    "when_failed": "-",
                    "thresh": "000",
                    "type": "Old_age",
                    "value": "100"
                },
                {
                    "raw": "9828",
                    "updated": "Always",
                    "num": "9",
                    "worst": "100",
                    "name": "Power_On_Hours",
                    "when_failed": "-",
                    "thresh": "000",
                    "type": "Old_age",
                    "value": "100"
                }
            ]
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
        step = int(100 / num)
        i = 1
        now = 0
        steps = ["0%"]
        partitions = []
        if role == s_fields.DiskPartitionRole.MIX:
            db = int(step / 5)
            db_size = disk_size * db / 100
            cache_size = disk_size * (step - db) / 100
            while i <= num:
                steps.append(str(now + db) + "%")
                partitions.append({
                    "name": name + str(i * 2 - 1),
                    "size": db_size,
                    "role": "db",
                })
                steps.append(str(now + step) + "%")
                partitions.append({
                    "name": name + str(i * 2),
                    "size": cache_size,
                    "role": "cache",
                })
                now += step
                i += 1
        else:
            partition_size = disk_size * step / 100
            while i <= num:
                now += step
                steps.append(str(now) + "%")
                partitions.append({
                    "name": name + str(i),
                    "size": partition_size,
                    "role": role,
                })
                i += 1
        logger.debug("Partition steps: {}".format(steps))
        return steps, partitions

    def disk_partitions_create(self, ctxt, node, disk, values):
        logger.debug('Make cache disk partition: %s', disk.name)
        ssh_client = self._get_ssh_executor(node)
        if not ssh_client:
            return []
        disk_tool = DiskTool(ssh_client)
        partition_num = values.get('partition_num')
        partition_role = values.get('partition_role')
        steps, partitions = self._get_disk_partition_steps(
            partition_num, partition_role, disk.name, disk.disk_size)
        try:
            disk_tool.partitions_clear(disk.name)
            disk_tool.partitions_create(disk.name, steps)
            logger.debug("Partitions: {}".format(partitions))
            return partitions
        except exception.StorException as e:
            logger.error("Create partitions error: {}".format(e))
            return []

    def disk_partitions_remove(self, ctxt, node, name):
        logger.debug('Remove cache disk partitions: %s', name)
        ssh_client = self._get_ssh_executor(node)
        if not ssh_client:
            return False
        disk_tool = DiskTool(ssh_client)
        try:
            _success = disk_tool.partitions_clear(name)
        except exception.StorException as e:
            logger.error("Create partitions error: {}".format(e))
            _success = False
        return _success